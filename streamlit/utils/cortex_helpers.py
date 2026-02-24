"""
Cortex AI Helpers
==================
Utility functions for interacting with Snowflake Cortex AI features.
"""

from typing import Optional


def generate_nl_to_sql(session, question: str) -> str:
    """
    Use Cortex COMPLETE to translate a natural language question into SQL.
    """
    safe_question = question.replace("'", "''")
    result = session.sql(f"""
        SELECT ADJUSTMENT_DB.AI.NL_TO_SQL('{safe_question}') AS SQL_QUERY
    """).collect()

    if result:
        sql = result[0]["SQL_QUERY"].strip()
        # Strip markdown fences if present
        if sql.startswith("```"):
            lines = sql.split("\n")
            sql = "\n".join(l for l in lines if not l.strip().startswith("```")).strip()
        return sql
    return ""


def explain_adjustment(session, adj_details: str) -> str:
    """
    Use Cortex COMPLETE to generate a business-friendly explanation.
    """
    safe_details = adj_details.replace("'", "''")[:2000]
    result = session.sql(f"""
        SELECT ADJUSTMENT_DB.AI.EXPLAIN_ADJUSTMENT('{safe_details}') AS EXPLANATION
    """).collect()
    return result[0]["EXPLANATION"] if result else ""


def classify_risk(session, business_reason: str, amount_impact: float) -> str:
    """
    Use Cortex to classify adjustment risk as LOW, MEDIUM, or HIGH.
    """
    safe_reason = business_reason.replace("'", "''")[:500]
    result = session.sql(f"""
        SELECT ADJUSTMENT_DB.AI.CLASSIFY_ADJUSTMENT_RISK('{safe_reason}', {amount_impact}) AS RISK
    """).collect()
    return result[0]["RISK"].strip().upper() if result else "UNKNOWN"


def summarize_adjustments(session, time_period: str = "this week") -> str:
    """
    Use Cortex COMPLETE to generate a summary of recent adjustments.
    """
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            'Summarize the following adjustment activity in 3-4 bullet points. '
            || 'Focus on: count, types, largest impacts, any reversals. '
            || 'Data: ' || (
                SELECT LISTAGG(
                    'ADJ#' || ADJ_ID || ' ' || ADJ_TYPE || ' ' || ADJ_STATUS
                    || ' rows=' || AFFECTED_ROWS || ' reason=' || LEFT(BUSINESS_REASON, 50),
                    '; '
                )
                FROM ADJUSTMENT_DB.CORE.ADJ_HEADER
                WHERE CREATED_AT >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
            )
        ) AS SUMMARY
    """).collect()
    return result[0]["SUMMARY"] if result else "No data available for summary."


def run_anomaly_detection(session) -> str:
    """
    Run the anomaly detection stored procedure and return results.
    """
    result = session.sql(
        "CALL ADJUSTMENT_DB.AI.SP_DETECT_ADJUSTMENT_ANOMALIES()"
    ).collect()
    return result[0][0] if result else "No results"


def generate_impact_narrative(
    session,
    adj_type: str,
    target_date: str,
    rows_affected: int,
    total_delta: float,
    business_reason: str,
    filter_criteria: str,
) -> str:
    """
    Generate a natural language narrative of an adjustment's impact.
    """
    prompt = (
        f"Write a concise 2-sentence business impact statement for this adjustment: "
        f"Type={adj_type}, Date={target_date}, Rows={rows_affected}, "
        f"Total Delta=${total_delta:,.2f}, Reason={business_reason}, "
        f"Scope={filter_criteria}"
    ).replace("'", "''")

    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{prompt}') AS NARRATIVE
    """).collect()
    return result[0]["NARRATIVE"] if result else ""
