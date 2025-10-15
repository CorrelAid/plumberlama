
# U25 Survey Data Processing Pipeline

It's lama with one l! Generate documentation for repeated cross-sectional (i.e. different/anonymous participants) surveys  created with Lamapoll and process results to simplify self-service data analysis and visualization.


## Quick Start

Run the complete pipeline:

```bash
uv run python cli.py all
```

Or run individual steps:

```bash
# Step 1: Process data from API
uv run python cli.py process

# Step 2: Generate documentation
uv run python cli.py docs

# Step 3: Load to database
uv run python cli.py load
```

## CLI Commands

### `process`
Fetch and process survey data from LamaPoll API:
- Determines question types and infers schema
- Filters invalid responses
- Generates semantic variable names using LLM
- Validates and casts data types
- Caches results for subsequent steps

```bash
uv run python cli.py process [OPTIONS]

Options:
  --poll-id INTEGER  LamaPoll poll ID (default: 1850964)
  --cache/--no-cache Cache processed data (default: true)
```

### `docs`
Generate MkDocs documentation from processed data:

```bash
uv run python cli.py docs [OPTIONS]

Options:
  -o, --output PATH  Output directory (default: docs)
```

### `load`
Load processed data into database:

```bash
uv run python cli.py load [OPTIONS]

Options:
  --env [DEV|PROD]           Database environment (default: DEV)
  --prefix TEXT              Table name prefix (default: u25_survey)
  --if-exists [replace|append|fail]  Action if tables exist (default: replace)
```

### `all`
Run complete pipeline (process → docs → load):

```bash
uv run python cli.py all [OPTIONS]

Options:
  --poll-id INTEGER   LamaPoll poll ID
  --env [DEV|PROD]    Database environment
```

## Pipeline

1. Using the `/questions` endpoint for the relevant poll, determine question types
  - LamaPoll has its own question types, but we further distinguish its types
    - The assumptions that come with determining question types are verified: it determines how many variables are associated with a question and what types these variables have
  - We are infering a schema that the poll results will be cast to/checked against
    - We create new variable names based on which question a variable belong to and generate suffixes based on labels so that it encodes more information
    - Some variables can only take predefined values, we add this extract this info and add it to the schema

2. Using the `/legacyResults` endpoint for the relevant poll, retrieve and process the results
  - Filter out completely empty responses and incomplete responses
  - We process data so that a check against the inferred schema succeeds#
    - decode integer vars that represent choices to their representations
    - for questions that allow specifying "other" values, create a new variable that holds the text input and keep one var that is the boolean of whether other was specified

3. We generate documentation that lists all variables, their type and associated questions and question types

4. We save the processed results and metadata to a database
  - In DEV mode: Uses local DuckDB database (fast, serverless, no setup required)
  - In PROD mode: Configurable for production database (PostgreSQL, etc.)

### Repeated pipeline runs

- in the config, we can specify a survey_id (e.g. u25_survey) for the repeated survey
  - this can be arbitrarily set and is stable across potentially multiple survey iterations (poll id can be different)
  - at the beginning of the pipeline, we check if there are already tables in the database
    - if thats not the case, we create new tables for metadata and results -> we add add a column load_counter = 0 for all result rows
    - if thats the case, we infer the metadata for the current poll id and then compare the metadata in the db with the new metadata
      - if different, we raise
      - if not, we continue with fetching and processing results but append to the existing tables -> we add add a column load_counter = max(load_counter)+1 for all result rows

## Database Storage

The pipeline automatically saves results to a database. The environment is controlled by the `ENV` variable in your `.env` file:

```bash
# In .env file
ENV=DEV  # Use local DuckDB (default)
# ENV=PROD  # Use production database
```

### DEV Mode (DuckDB)

By default, the pipeline uses DuckDB for local development:

- Database location: `data/survey_results.duckdb`
- Three tables are created:
  - `u25_survey_results`: Survey responses (one row per respondent)
  - `u25_survey_variables`: Variable metadata
  - `u25_survey_questions`: Question metadata

### Querying the Database

You can query the DuckDB database directly:

```python
from plumberlama.database import query_database

# Get all results
results = query_database("SELECT * FROM u25_survey_results")

# Get variable metadata
variables = query_database("SELECT * FROM u25_survey_variables WHERE question_type = 'single_choice'")
```

Or use DuckDB CLI:

```bash
duckdb data/survey_results.duckdb
```

## Documentation

The project uses MkDocs with Material theme to generate searchable documentation.

### Viewing the Documentation

After running `main.py` to generate the documentation:

```bash
# Preview locally with live reload
uv run mkdocs serve

# Build static site
uv run mkdocs build
```

The documentation will be available at http://localhost:8000

### Deploying to GitHub Pages

The documentation is automatically deployed to GitHub Pages when you push to the main branch. The workflow is defined in `.github/workflows/deploy-docs.yml`.

You can also manually deploy:

```bash
uv run mkdocs gh-deploy
```

# Lamapoll Investigations and Thoughts

### Output Formats

- Lamapolls V2 API only offers retrieving aggregated results per question. Thats a suitable output format for univariate analysis (statistics are even provided directly), but not optimal for multivariate methods.

- However the API has an endpoint to retrieve V1 compatible legacy results that returns a data where one row equals one participant. This long format allows multivariate methods and is **tidy data**

- **Tidy data**:
  - Every column is a variable.
  - Every row is an observation (one respondent).
  - Every cell holds a single value.

- The Lamapoll data schema contains the entity question, group, varnames, items and labels.
  - questions have a type
  - questions can have multiple groups
  - groups can have multiple varnames, multiple items and multiple labels
  - items have one input type that can be null
  - items and varnames are not equal
  - when there are multiple items question type is matrix
  - labels are answer options, but do not have to equal variables....
  - labels can be empty
  - in case of matrices, labels are possible values a set of vars can take, same with single choice
  - labels are baiscally values a variable can take

- Entity Question does not actually mean question in the language sense, its more of a **task**, e.g. "rate the following statements"
- Most fitting entity of analysis are **variables**
  - Variables:
    - synonym: **data point**
    - holds one value per respondent


# Survey Design Feedback
- The fact that nothing is mandatory is not ideal (empty reponses possible)
- there are basically two types of single choice (dropdown and radio buttons where only choice is possible), but they are the same in the data
- gender is multiple choice (on purpose?)

# Followed Design Patterns/paradigms
- Functional Programming
  - declarative programming
  - using pure functions that have no side effects (memory or I/O)
  - explicit Data Flow
- Contract Programming
  - Pre-Conditions and Post-Conditions
- Data-Oriented Programming
  - Separate data from code
  - Represent data with generic data structures
  - Data is immutable
    - Keep Data Immutable
    - Data never changes after creation
    - Modifications create new versions
  - Separate data schema from representation


 if_exists == "replace" should not be possible. either create or append. maybe rename
  this variable to something more fitting
