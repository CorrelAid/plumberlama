def get_question_metadata(table_prefix: str, question_id: int) -> str:
    """Get comprehensive metadata for a specific question."""
    return f"""
    SELECT
        id as variable_name,
        label as variable_label,
        question_text,
        question_type,
        possible_values_labels,
        scale_labels,
        range_min,
        range_max
    FROM {table_prefix}_metadata
    WHERE question_id = {question_id}
    ORDER BY question_position, group_id
    """


def get_frequency_distribution(
    table_prefix: str, variable_name: str, include_nulls: bool = False
) -> str:
    """Calculate frequency distribution with percentages."""
    where_clause = "" if include_nulls else f'WHERE "{variable_name}" IS NOT NULL'

    return f"""
    SELECT
        "{variable_name}" as response,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM {table_prefix}_results
    {where_clause}
    GROUP BY "{variable_name}"
    ORDER BY count DESC
    """


def get_time_series_analysis(
    table_prefix: str, variable_name: str, aggregation: str = "AVG"
) -> str:
    """Analyze trends across multiple waves using load_counter."""
    return f"""
    SELECT
        load_counter as wave,
        {aggregation}(CAST("{variable_name}" AS FLOAT)) as avg_value,
        COUNT("{variable_name}") as response_count
    FROM {table_prefix}_results
    WHERE "{variable_name}" IS NOT NULL
    GROUP BY load_counter
    ORDER BY load_counter
    """


def get_matrix_question_metadata(
    table_prefix: str, question_type: str = "matrix"
) -> str:
    """Get metadata for matrix questions with scale labels."""
    return f"""
    SELECT
        m.id as variable_name,
        m.label as item_label,
        m.question_text,
        m.scale_labels,
        m.range_min,
        m.range_max
    FROM {table_prefix}_metadata m
    WHERE m.question_type = '{question_type}'
    ORDER BY m.question_id, m.group_id
    """


def get_matrix_question_responses(table_prefix: str, variable_name: str) -> str:
    """Get response distribution for a matrix question item."""
    return f"""
    SELECT
        "{variable_name}" as score,
        COUNT(*) as frequency
    FROM {table_prefix}_results
    WHERE "{variable_name}" IS NOT NULL
    GROUP BY "{variable_name}"
    ORDER BY "{variable_name}"
    """


def find_variable_by_question_type(
    table_prefix: str, question_type: str, limit: int = 1
) -> str:
    """Find variables of a specific question type."""
    return f"""
    SELECT id, question_text, possible_values_labels
    FROM {table_prefix}_metadata
    WHERE question_type = '{question_type}'
    LIMIT {limit}
    """
