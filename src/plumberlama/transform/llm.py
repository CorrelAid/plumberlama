from typing import Union

import dspy


def load_llm(llm_model: str, llm_key: str, llm_base_url: str):
    """Load and configure LLM for DSPy."""
    lm = dspy.LM(
        model=f"openai/{llm_model}",
        model_type="chat",
        temperature=0.3,
        api_key=llm_key,
        base_url=llm_base_url,
        cache=False,
        max_tokens=200,
    )
    return lm


examples = [
    # Example 1: Single-variable question (no previous names)
    dspy.Example(
        previous_variable_names=[],
        question_text="Nachfolgend findest du einige Aussagen zu [U25]. Bitte bewerte diese auf der Skala:",
        variable_text="Die Peer-Ausbildung [U25] hat mich gut vorbereitet.",
        variable_suffix="vorbereitet",
    ).with_inputs("previous_variable_names", "question_text", "variable_text"),
    # Example 2: First variable in multi-variable question
    dspy.Example(
        previous_variable_names=[],
        question_text="Wie bist du zu diesem Ehrenamt gekommen?",
        variable_text="Eine andere Person hat mich mitgenommen, bzw. mir von diesem Ehrenamt erzählt.",
        variable_suffix="erzaehlung",
    ).with_inputs("previous_variable_names", "question_text", "variable_text"),
    # Example 3: Second variable in multi-variable question (has previous names)
    dspy.Example(
        previous_variable_names=["Q5_erstes"],
        question_text="Welche drei Worte verbindest du spontan mit [U25]?",
        variable_text="Zweites Textfeld:",
        variable_suffix="zweites",
    ).with_inputs("previous_variable_names", "question_text", "variable_text"),
    # Example 4: Third variable with multiple previous names
    dspy.Example(
        previous_variable_names=["Q6_internet", "Q6_freunde"],
        question_text="Wie bist du zu [U25] gekommen?",
        variable_text="Ich habe [U25] in einer anderen Organisation/ Einrichtung kennengelernt (z.B. Kirchengemeinde, Jugendgruppe, Beratungsstelle, …).",
        variable_suffix="organisation",
    ).with_inputs("previous_variable_names", "question_text", "variable_text"),
]


def make_generator():
    """Create DSPy generator for variable names."""

    variable_suffix_desc = """Generate a descriptive suffix using EXACTLY ONE existing dictionary word (lowercase and letters only, no numbers, umlauts like ä, ö, ü, ß are NOT allowed).
IMPORTANT: The suffix must be a single real German/English word that exists in a dictionary and accurately describes the variable in the context of the question. Never use numbers.
Never use underscores. Never use multiple words. Just one meaningful word that captures the essence of the variable_text.
"""

    class VariableGenerator(dspy.Signature):
        """Given a question text and the text associated with a variable, generates a descriptive variable name suffix. Adapt to language of input text."""

        previous_variable_names: Union[list, None] = dspy.InputField()
        question_text: str = dspy.InputField()
        variable_text: str = dspy.InputField()
        variable_suffix: str = dspy.OutputField(desc=variable_suffix_desc)

    teleprompter = dspy.LabeledFewShot()
    term_parser = teleprompter.compile(
        student=dspy.Predict(VariableGenerator), trainset=examples
    )

    return term_parser
