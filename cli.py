"""U25 Survey Data Processing CLI."""

import click
import os
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich.table import Table
from dotenv import load_dotenv
import polars as pl
import polars.selectors as cs
import io
import requests

from lib.helpers import process_questions_and_variables
from lib.schema import SurveySchema
from lib.doc import create_survey_documentation
from lib.label_generation import load_llm, make_generator, rename_vars_with_labels
from lib.database import save_to_database, query_database

load_dotenv()

console = Console()

# Global config
LAMA_API_TOKEN = os.getenv('SECRET_LAMA_API_TOKEN')
BASE_URL = "https://app.lamapoll.de/api/v2"
POLL_ID = 1850964
CACHE_DIR = Path("cache")


@click.group()
@click.version_option(version="0.1.0")
@click.pass_context
def cli(ctx):
    """U25 Survey Data Processing Pipeline.

    Process survey data from LamaPoll API, generate documentation, and load into database.
    """
    ctx.ensure_object(dict)


@cli.command()
@click.option('--poll-id', default=POLL_ID, help='LamaPoll poll ID')
@click.option('--cache/--no-cache', default=True, help='Cache processed data')
@click.pass_context
def process(ctx, poll_id, cache):
    """Process survey data from LamaPoll API.

    Fetches questions and results, determines question types, infers schema,
    filters/validates responses, and renames variables.
    """
    if cache:
        CACHE_DIR.mkdir(exist_ok=True)

    console.print(Panel.fit("🔄 Processing Survey Data", style="bold blue"))

    headers = {
        "Authorization": f"Bearer {LAMA_API_TOKEN}",
        "Content-Type": "application/json"
    }

    # Step 1: Fetch and process questions
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task("Fetching questions from LamaPoll API...", total=None)

        response = requests.get(f"{BASE_URL}/polls/{poll_id}/questions", headers=headers)
        response.raise_for_status()
        questions = response.json()

        progress.update(task, description="Processing question types...")
        question_df, variable_df = process_questions_and_variables(questions)

        progress.update(task, description="✓ Questions processed", completed=True)

    console.print(f"  📊 Found [cyan]{len(question_df)}[/cyan] questions with [cyan]{len(variable_df)}[/cyan] variables")

    # Step 2: Fetch and process results
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task("Fetching results from LamaPoll API...", total=None)

        response = requests.get(f"{BASE_URL}/polls/{poll_id}/legacyResults", headers=headers)
        response.raise_for_status()
        legacy = response.json()

        progress.update(task, description="Loading and filtering responses...")
        results_df = pl.read_csv(io.StringIO(legacy["data"]))

        # Filter out incomplete and empty responses
        results_df = results_df.filter(
            (pl.col("vCOMPLETED") != 0) &
            ~pl.all_horizontal(
                cs.matches("^V\\d").fill_null("").cast(pl.String, strict=False) == ""
            )
        )

        progress.update(task, description="✓ Results loaded and filtered", completed=True)

    console.print(f"  ✅ Loaded [cyan]{len(results_df)}[/cyan] valid responses")

    # Step 3: Generate variable names with LLM
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task("Initializing LLM for variable naming...", total=None)

        llm_model = os.getenv("LLM_MODEL", "mistralai/devstral-medium")
        llm_key = os.getenv("OR_KEY")
        llm_base_url = "https://openrouter.ai/api/v1"

        load_llm(llm_model, llm_key, llm_base_url)
        generator = make_generator()

        progress.update(task, description="Generating semantic variable names...")
        results_df, variable_df = rename_vars_with_labels(results_df, variable_df, generator, question_df)

        progress.update(task, description="✓ Variables renamed", completed=True)

    # Step 4: Build schema and validate
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task("Building schema and validating...", total=None)

        schema = SurveySchema(variable_df)
        results_df = schema.validate_and_cast(results_df, validate=True)
        results_df = schema.decode_single_choice(results_df)

        progress.update(task, description="✓ Schema built and validated", completed=True)

    # Save to cache
    if cache:
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
            task = progress.add_task("Caching processed data...", total=None)

            results_df.write_parquet(CACHE_DIR / "results.parquet")

            progress.update(task, description="✓ Data cached", completed=True)

    console.print("\n[bold green]✓ Processing complete![/bold green]")

    # Display summary table
    table = Table(title="Processing Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")

    table.add_row("Questions", str(len(question_df)))
    table.add_row("Variables", str(len(variable_df)))
    table.add_row("Responses", str(len(results_df)))
    table.add_row("Columns", str(len(results_df.columns)))

    console.print(table)

    # Store data in context for use by other commands
    ctx.obj = {
        'results_df': results_df,
        'variable_df': variable_df,
        'question_df': question_df
    }


@cli.command()
@click.option('--output', '-o', default='docs', help='Output directory for documentation')
@click.pass_context
def docs(ctx, output):
    """Generate MkDocs documentation from processed data.

    Creates comprehensive documentation with question details, variable metadata,
    and searchable content.
    """
    console.print(Panel.fit("📚 Generating Documentation", style="bold blue"))

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task("Loading data...", total=None)

        # Try to get data from context (if run via 'all' command)
        if ctx.obj and 'results_df' in ctx.obj:
            results_df = ctx.obj['results_df']
            variable_df = ctx.obj['variable_df']
            question_df = ctx.obj['question_df']
        else:
            # Load from cache if available
            if not CACHE_DIR.exists() or not (CACHE_DIR / "results.parquet").exists():
                console.print("[red]Error: No cached data found. Run 'process' first.[/red]")
                raise click.Abort()

            results_df = pl.read_parquet(CACHE_DIR / "results.parquet")

            # Re-process questions to get metadata
            headers = {
                "Authorization": f"Bearer {LAMA_API_TOKEN}",
                "Content-Type": "application/json"
            }
            response = requests.get(f"{BASE_URL}/polls/{POLL_ID}/questions", headers=headers)
            response.raise_for_status()
            questions = response.json()
            question_df, variable_df = process_questions_and_variables(questions)

            # Re-run variable naming
            llm_model = os.getenv("LLM_MODEL", "mistralai/devstral-medium")
            llm_key = os.getenv("OR_KEY")
            llm_base_url = "https://openrouter.ai/api/v1"
            load_llm(llm_model, llm_key, llm_base_url)
            generator = make_generator()
            results_df, variable_df = rename_vars_with_labels(results_df, variable_df, generator, question_df)

        schema = SurveySchema(variable_df)

        progress.update(task, description="Generating documentation...", total=None)

        create_survey_documentation(variable_df, question_df, results_df, schema, output)

        progress.update(task, description="✓ Documentation generated", completed=True)

    console.print(f"\n[bold green]✓ Documentation generated in '{output}/' directory[/bold green]")
    console.print(f"\n💡 Preview with: [cyan]uv run mkdocs serve[/cyan]")
    console.print(f"💡 Build with: [cyan]uv run mkdocs build[/cyan]")


@cli.command()
@click.option('--env', type=click.Choice(['DEV', 'PROD']), default='DEV', help='Environment (DEV=DuckDB, PROD=Production DB)')
@click.option('--prefix', default='u25_survey', help='Table name prefix')
@click.option('--if-exists', type=click.Choice(['replace', 'append', 'fail']), default='replace', help='What to do if tables exist')
@click.pass_context
def load(ctx, env, prefix, if_exists):
    """Load processed data into database.

    Saves survey results and metadata to database. Uses DuckDB for DEV environment
    and configurable production database for PROD.
    """
    console.print(Panel.fit(f"💾 Loading Data to Database ({env})", style="bold blue"))

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task("Loading data...", total=None)

        # Try to get data from context (if run via 'all' command)
        if ctx.obj and 'results_df' in ctx.obj:
            results_df = ctx.obj['results_df']
            variable_df = ctx.obj['variable_df']
            question_df = ctx.obj['question_df']
        else:
            # Load from cache if available
            if not CACHE_DIR.exists() or not (CACHE_DIR / "results.parquet").exists():
                console.print("[red]Error: No cached data found. Run 'process' first.[/red]")
                raise click.Abort()

            results_df = pl.read_parquet(CACHE_DIR / "results.parquet")

            # Re-process questions to get metadata
            headers = {
                "Authorization": f"Bearer {LAMA_API_TOKEN}",
                "Content-Type": "application/json"
            }
            response = requests.get(f"{BASE_URL}/polls/{POLL_ID}/questions", headers=headers)
            response.raise_for_status()
            questions = response.json()
            question_df, variable_df = process_questions_and_variables(questions)

            # Re-run variable naming
            llm_model = os.getenv("LLM_MODEL", "mistralai/devstral-medium")
            llm_key = os.getenv("OR_KEY")
            llm_base_url = "https://openrouter.ai/api/v1"
            load_llm(llm_model, llm_key, llm_base_url)
            generator = make_generator()
            results_df, variable_df = rename_vars_with_labels(results_df, variable_df, generator, question_df)

        progress.update(task, description="Saving to database...", total=None)

        save_to_database(
            results_df=results_df,
            variable_df=variable_df,
            question_df=question_df,
            table_prefix=prefix,
            env=env,
            if_exists=if_exists
        )

        progress.update(task, description="✓ Data saved to database", completed=True)

    console.print(f"\n[bold green]✓ Data loaded successfully![/bold green]")

    if env == "DEV":
        console.print(f"\n💡 Query with: [cyan]duckdb data/survey_results.duckdb[/cyan]")
        console.print(f"💡 Or use Python: [cyan]from lib.database import query_database[/cyan]")


@cli.command()
@click.option('--poll-id', default=POLL_ID, help='LamaPoll poll ID')
@click.option('--env', type=click.Choice(['DEV', 'PROD']), default='DEV', help='Database environment')
@click.pass_context
def all(ctx, poll_id, env):
    """Run complete pipeline: process → docs → load.

    Executes all three steps in sequence.
    """
    console.print(Panel.fit("🚀 Running Complete Pipeline", style="bold blue"))

    # Initialize context object for sharing data between commands
    ctx.ensure_object(dict)

    # Step 1: Process
    ctx.invoke(process, poll_id=poll_id, cache=True)
    console.print()

    # Step 2: Generate docs
    ctx.invoke(docs, output='docs')
    console.print()

    # Step 3: Load to database
    ctx.invoke(load, env=env, prefix='u25_survey', if_exists='replace')

    console.print(f"\n[bold green]✨ Complete pipeline finished successfully! ✨[/bold green]")


if __name__ == '__main__':
    cli()
