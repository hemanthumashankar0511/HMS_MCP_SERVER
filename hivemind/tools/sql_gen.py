from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def handle_text_to_hiveql(natural_query: str, assembled_context: str) -> str:
    """
    Formats the schema/partition context and user query into a structured prompt
    that instructs the LLM to produce a safe, well-formed HiveQL query.
    """
    try:
        if not natural_query.strip():
            return "Error: natural_query cannot be empty."
        if not assembled_context.strip():
            return (
                "Error: assembled_context is empty. "
                "Run search_tables and get_table_schema first to gather metadata, "
                "then pass that output here."
            )

        lines = [
            "You MUST produce a complete, runnable HiveQL query for the request below.",
            "Do NOT say 'I cannot execute this' or 'only metadata is available'.",
            "The user will run the query themselves — your job is ONLY to write it.",
            "",
            f"Request: {natural_query.strip()}",
            "",
            "Schema and partition context (from HMS metastore):",
            assembled_context.strip(),
            "",
            "Rules:",
            "- Use backticks around all database, table, and column names.",
            "- If partition keys are present, include a WHERE clause filtering on them.",
            "- Use explicit JOIN ... ON syntax with short aliases when joining tables.",
            "- Add LIMIT 100 unless the request specifies otherwise.",
            "- Write only a SELECT query (no INSERT, UPDATE, DROP, or DDL).",
            "- Base all column and table names strictly on the context above.",
            "- Estimate rows affected using num_rows from stats context if available.",
            "  Compare filtered vs unfiltered to give a meaningful estimate.",
            "",
            "Your response MUST follow this EXACT format — no deviations:",
            "",
            "```sql",
            "[complete HiveQL query — no placeholders]",
            "```",
            "Tables used: [database.table, ...]",
            "Partition filter: [key=value applied / None]",
            "Estimated rows: [number or range based on stats, with brief reason]",
        ]

        return "\n".join(lines)

    except Exception as exc:
        logger.exception("text_to_hiveql failed")
        return f"Error preparing SQL generation context: {exc}"
