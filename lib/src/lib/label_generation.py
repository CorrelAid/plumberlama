import polars as pl
import dspy
import asyncio
from typing import Union

examples = [
    dspy.Example(
        previous_variable_names=None,
        question_text="Nachfolgend findest du einige Aussagen zu [U25]. Bitte bewerte diese auf der Skala:",
        variable_text="Die Peer-Ausbildung [U25] hat mich gut vorbereitet.",
        variable_suffix="vorbereitet"
    ).with_inputs("previous_variable_names", "question_text", "variable_text"),
    dspy.Example(
        previous_variable_names=None,
        question_text="Wie bist du zu diesem Ehrenamt gekommen?",
        variable_text="Eine andere Person hat mich mitgenommen, bzw. mir von diesem Ehrenamt erzählt.",
        variable_suffix="erzählung"
    ).with_inputs("previous_variable_names", "question_text", "variable_text"),
    dspy.Example(
        previous_variable_names=None,
        question_text="Welche drei Worte verbindest du spontan mit [U25]?",
        variable_text="Erstes Textfeld:",
        variable_suffix="erstes"
    ).with_inputs("previous_variable_names", "question_text", "variable_text"),
    dspy.Example(
        previous_variable_names=None,
        question_text="Wie bist du zu [U25] gekommen?",
        variable_text="Ich habe [U25] in einer anderen Organisation/ Einrichtung kennengelernt (z.B. Kirchengemeinde, Jugendgruppe, Beratungsstelle, …).",
        variable_suffix="organisation"
    ).with_inputs("previous_variable_names", "question_text", "variable_text")
]


def load_llm(llm_model: str, llm_key: str, llm_base_url: str):
    """Load and configure LLM for DSPy."""
    lm = dspy.LM(
        model=f"openai/{llm_model}",
        model_type="chat",
        temperature=0.3,
        api_key=llm_key,
        base_url=llm_base_url,
        cache=False,
        max_tokens=200,  # Allow room for reasoning and full variable name suffix generation
    )
    dspy.configure(lm=lm)
    return lm


def make_generator():
    """Create DSPy generator for variable names."""
    class VariableGenerator(dspy.Signature):
        """Given a question text and the text associated with a variable, generates a descriptive variable name suffix. Adapt to language of input text."""

        previous_variable_names: Union[list,None] = dspy.InputField()
        question_text: str = dspy.InputField()
        variable_text: str = dspy.InputField()
        variable_suffix: str = dspy.OutputField(desc="Generate a descriptive suffix using EXACTLY ONE existing dictionary word (lowercase only, umlauts like ä, ö, ü, ß are allowed). IMPORTANT: The suffix must be a single real German/English word that exists in a dictionary and accurately describes the variable in the context of the question. Never use numbers. Never use underscores. Never use multiple words. Just one meaningful word that captures the essence of the variable_text.")

    teleprompter = dspy.LabeledFewShot()
    term_parser = teleprompter.compile(
        student=dspy.Predict(VariableGenerator), trainset=examples
    )

    return term_parser


async def gen_var_name_async(generator, question_text, question_position, variable_text, previous_variable_names, is_other_boolean=False, is_other_text=False, max_retries=3):
    """Generate variable name using LLM (async) with validation for no numbers."""
    import re

    loop = asyncio.get_event_loop()

    for attempt in range(max_retries):
        # Add context about names to avoid
        prompt_suffix = ""
        if previous_variable_names:
            # Filter out AVOID: prefix markers and extract just the suffix part
            avoid_names = [n.replace('AVOID:', '').split('_', 1)[1] if '_' in n else n
                          for n in previous_variable_names if n]
            prompt_suffix = f" IMPORTANT: Generate a DIFFERENT suffix than these already used: {', '.join(avoid_names)}."

        # Add prompt context
        modified_question_text = question_text + prompt_suffix

        result = await loop.run_in_executor(
            None,
            lambda: generator(
                previous_variable_names=previous_variable_names,
                question_text=modified_question_text,
                variable_text=variable_text
            )
        )

        # Get suffix and build full name with question position
        suffix = result.variable_suffix.strip().lstrip("_")

        # Validate: suffix must be a single word (no underscores, no numbers, only letters including umlauts)
        # Allow lowercase letters including German umlauts and other unicode letters
        if not re.match(r'^[a-zäöüß]+$', suffix, re.UNICODE):
            if attempt < max_retries - 1:
                continue  # Retry
            else:
                # On final attempt, assert valid format
                assert False, f"LLM generated invalid suffix '{suffix}' for variable_text '{variable_text}' after {max_retries} attempts. Variable suffixes must be a single word containing only lowercase letters including umlauts (no numbers, no underscores, no special characters)."

        var_name = f"Q{question_position}_{suffix}"

        # Apply additional suffix for 'other' variables
        if is_other_text:
            if not var_name.endswith("_other_text"):
                if var_name.endswith("_other"):
                    var_name = var_name + "_text"
                else:
                    var_name = var_name + "_other_text"
        elif is_other_boolean and not var_name.endswith("_other"):
            var_name = var_name + "_other"

        return var_name

    # Should never reach here
    return var_name


def gen_var_name(generator, question_text, question_position, variable_text, previous_variable_names, is_other_boolean=False, is_other_text=False):
    """Generate variable name using LLM (sync wrapper)."""
    return asyncio.run(gen_var_name_async(
        generator, question_text, question_position, variable_text,
        previous_variable_names, is_other_boolean, is_other_text
    ))



def deterministic_var_name(question_position, is_other_boolean=False, is_other_text=False):
    """
    Generate a deterministic variable name for single-variable questions without using LLM.

    Args:
        question_position: Position of question in survey
        is_other_boolean: Whether this is the boolean checkbox for "Other" option
        is_other_text: Whether this is the text field for "Other" option

    Returns:
        Variable name in format Q<position> or Q<position>_<suffix>
    """
    base_name = f"Q{question_position}"

    # Apply suffix for 'other' variables
    if is_other_text:
        return f"{base_name}_other_text"
    elif is_other_boolean:
        return f"{base_name}_other"

    return base_name


async def _process_question_vars_async(generator, question_vars, question_text, question_position, all_generated_names, rename_mapping):
    """Process variables for a single question with async LLM calls."""
    question_var_names = []
    var_index = 0

    for var in question_vars:
        label = var.get("label", "")
        question_type = var.get("question_type", "")

        # For "other" variables in multiple_choice_other questions, use deterministic naming
        if question_type == "multiple_choice_other" and (var.get("is_other_text") or var.get("is_other_boolean")):
            base_name = f"Q{question_position}"
            if var.get("is_other_text"):
                new_var_id = f"{base_name}_other_text"
            else:  # must be is_other_boolean since we checked the OR condition above
                new_var_id = f"{base_name}_other"

        # For variables with labels (including regular choices in multiple_choice_other), use LLM
        elif label:
            assert label.strip(), f"Label must not be empty or whitespace-only for variable {var.get('id')}"

            # Try up to 3 times to get a unique name
            max_attempts = 3
            new_var_id = None
            for attempt in range(max_attempts):
                new_var_id = await gen_var_name_async(
                    generator=generator,
                    question_text=question_text,
                    question_position=question_position,
                    variable_text=label,
                    previous_variable_names=question_var_names,
                    is_other_boolean=var.get("is_other_boolean", False),
                    is_other_text=var.get("is_other_text", False)
                )

                # Check if unique across ALL generated names (not just this question)
                if new_var_id not in all_generated_names and new_var_id not in question_var_names:
                    break  # Success

                # On retry, append the failed attempt to previous_variable_names
                if attempt < max_attempts - 1:
                    question_var_names.append(f"AVOID:{new_var_id}")

            # Assert uniqueness - no fallback allowed
            assert new_var_id not in all_generated_names and new_var_id not in question_var_names, \
                f"Failed to generate unique variable name for label '{label}'. Generated '{new_var_id}' which already exists. LLM failed to respect previous_variable_names context after {max_attempts} attempts."
        # For variables without labels, raise an error (no fallback to numbers)
        else:
            raise ValueError(
                f"Variable {var.get('id')} has no label. All variables in multi-variable questions must have labels. "
                f"Question position: {question_position}, variable_text: {var.get('label', '')}"
            )

        # Assert no duplicates - applies to all variables (LLM and non-LLM)
        assert new_var_id not in all_generated_names, \
            f"Duplicate variable name '{new_var_id}' generated for variable {var.get('id')}. This should never happen."

        question_var_names.append(new_var_id)
        all_generated_names.add(new_var_id)
        rename_mapping[var["id"]] = new_var_id
        var_index += 1


def rename_vars_with_labels(results_df, variable_df, generator, question_df):
    """Rename columns to Q<position>_<suffix> format using LLM and deterministic naming."""

    async def _async_rename():
        # Count variables per question
        vars_per_question = variable_df.group_by("question_id").agg(pl.count("id").alias("var_count"))

        rename_mapping = {}
        all_generated_names = set()  # Track all names to detect duplicates

        # Group variables by question for parallel processing
        variables_by_question = {}
        for var in variable_df.to_dicts():
            if var["id"] not in results_df.columns:
                continue
            # Add 'label' key if it doesn't exist
            if "label" not in var:
                var["label"] = ""
            question_id = var["question_id"]
            if question_id not in variables_by_question:
                variables_by_question[question_id] = []
            variables_by_question[question_id].append(var)

        # Prepare tasks for all multi-variable questions (run in parallel)
        question_tasks = []
        for question_id, question_vars in variables_by_question.items():
            var_count = vars_per_question.filter(pl.col("question_id") == question_id)["var_count"][0]

            # For single-variable questions, use deterministic naming
            if var_count == 1:
                var = question_vars[0]
                new_var_id = deterministic_var_name(
                    question_position=var["question_position"],
                    is_other_boolean=var.get("is_other_boolean", False),
                    is_other_text=var.get("is_other_text", False)
                )
                rename_mapping[var["id"]] = new_var_id
                all_generated_names.add(new_var_id)

            # For multi-variable questions, create async task
            elif var_count > 1:
                question_text = question_df.filter(pl.col("id") == question_id)["text"][0]
                question_position = question_vars[0]["question_position"]

                task = _process_question_vars_async(
                    generator, question_vars, question_text,
                    question_position, all_generated_names, rename_mapping
                )
                question_tasks.append(task)

        # Process all multi-variable questions in parallel
        if question_tasks:
            await asyncio.gather(*question_tasks)

        return rename_mapping

    # Run async code
    rename_mapping = asyncio.run(_async_rename())

    # Check for duplicate target names in rename_mapping
    target_names = list(rename_mapping.values())
    duplicate_targets = [name for name in set(target_names) if target_names.count(name) > 1]
    if duplicate_targets:
        # Find which source variables map to the same target
        conflicts = {}
        for source, target in rename_mapping.items():
            if target in duplicate_targets:
                if target not in conflicts:
                    conflicts[target] = []
                conflicts[target].append(source)
        raise ValueError(f"Duplicate target variable names detected: {conflicts}")

    # Apply renaming to results_df
    if rename_mapping:
        results_df = results_df.rename(rename_mapping)

    # Update variable_df with renamed IDs
    if rename_mapping:
        variable_df = variable_df.with_columns(
            pl.col("id").replace_strict(rename_mapping, default=pl.col("id"), return_dtype=pl.String).alias("id")
        )

    # Assert that no V<number> syntax remains in column names
    v_pattern_cols = [col for col in results_df.columns if col.startswith("V") and any(c.isdigit() for c in col[1:3])]
    assert len(v_pattern_cols) == 0, f"Found columns with V<number> syntax that should have been renamed: {v_pattern_cols}"

    # Assert that there are no duplicate variable names
    duplicate_cols = [col for col, count in pl.Series(results_df.columns).value_counts().iter_rows() if count > 1]
    assert len(duplicate_cols) == 0, f"Found duplicate variable names in results_df: {duplicate_cols}"

    return results_df, variable_df

