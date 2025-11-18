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
        range_max,
        anonymization_type
    FROM {table_prefix}_metadata
    WHERE question_id = {question_id}
    ORDER BY question_position, group_id
    """


def get_frequency_distribution(
    table_prefix: str, variable_name: str, include_nulls: bool = False
) -> str:
    """Calculate frequency distribution with percentages from anonymized categorical data.

    This query aggregates counts from the categorical table where aggregated
    choice/boolean values are stored.
    """
    where_clause = "" if include_nulls else "AND value IS NOT NULL"

    return f"""
    SELECT
        value as response,
        SUM(CAST(count AS BIGINT)) as count,
        ROUND(SUM(CAST(count AS BIGINT)) * 100.0 / SUM(SUM(CAST(count AS BIGINT))) OVER (), 2) as percentage
    FROM {table_prefix}_categorical
    WHERE variable_id = '{variable_name}' {where_clause}
    GROUP BY value
    ORDER BY SUM(CAST(count AS BIGINT)) DESC
    """


def get_time_series_analysis(
    table_prefix: str, variable_name: str, aggregation: str = "AVG"
) -> str:
    """Analyze trends across multiple waves using load_counter from anonymized distributions.

    This query calculates statistics from the distributions table where shuffled
    numeric values (scale, matrix, numeric inputs) are stored.
    """
    return f"""
    SELECT
        load_counter as wave,
        {aggregation}(CAST(value AS FLOAT)) as avg_value,
        COUNT(value) as response_count
    FROM {table_prefix}_distributions
    WHERE variable_id = '{variable_name}' AND value IS NOT NULL
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


def get_distribution_stats(table_prefix: str, variable_name: str) -> str:
    """Get comprehensive statistics for a variable from anonymized distributions.

    Returns mean, median, std dev, min, max, and count for numeric variables
    (scale, matrix, numeric inputs) stored in the distributions table.
    """
    return f"""
    SELECT
        '{variable_name}' as variable_name,
        COUNT(value) as n,
        ROUND(CAST(AVG(CAST(value AS FLOAT)) AS NUMERIC), 2) as mean,
        ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(value AS FLOAT)) AS NUMERIC), 2) as median,
        ROUND(CAST(STDDEV(CAST(value AS FLOAT)) AS NUMERIC), 2) as std_dev,
        MIN(CAST(value AS FLOAT)) as min,
        MAX(CAST(value AS FLOAT)) as max
    FROM {table_prefix}_distributions
    WHERE variable_id = '{variable_name}' AND value IS NOT NULL
    """


def get_matrix_question_responses(table_prefix: str, variable_name: str) -> str:
    """Get response distribution for a matrix question item from anonymized distributions.

    Matrix questions use shuffled distributions to preserve privacy while
    maintaining statistical properties.
    """
    return f"""
    SELECT
        CAST(value AS INTEGER) as score,
        COUNT(*) as frequency
    FROM {table_prefix}_distributions
    WHERE variable_id = '{variable_name}' AND value IS NOT NULL
    GROUP BY value
    ORDER BY value
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
