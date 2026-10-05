"""Genie Conversation API proxy for the NexusRetail Analytics space.

Runs as the app's service principal; the space needs CAN_RUN for the SP (granted
by the bundle's genie_space app resource) and the SP needs SELECT on the space's
tables.
"""
from __future__ import annotations

from typing import Any

from . import config
from .db import client


def is_configured() -> bool:
    return bool(config.GENIE_SPACE_ID)


def ask(message: str, conversation_id: str | None = None) -> dict[str, Any]:
    """Ask Genie a question. Returns answer text, generated SQL, and a result preview."""
    if not is_configured():
        return {"configured": False, "answer": "Genie is not configured. Set genie_space_id in the bundle."}

    genie = client().genie
    space = config.GENIE_SPACE_ID
    if conversation_id:
        msg = genie.create_message_and_wait(space_id=space, conversation_id=conversation_id,
                                            content=message)
    else:
        msg = genie.start_conversation_and_wait(space_id=space, content=message)

    answer, sql, description, table = [], None, None, None
    for att in msg.attachments or []:
        if att.text and att.text.content:
            answer.append(att.text.content)
        if att.query:
            sql = att.query.query
            description = att.query.description
            res = genie.get_message_attachment_query_result(
                space_id=space, conversation_id=msg.conversation_id,
                message_id=msg.message_id or msg.id, attachment_id=att.attachment_id)
            sr = res.statement_response
            if sr and sr.manifest and sr.result and sr.result.data_array:
                table = {"columns": [c.name for c in sr.manifest.schema.columns],
                         "rows": sr.result.data_array[:100],
                         "row_count": sr.manifest.total_row_count}

    error = msg.error.error if msg.error else None
    return {
        "configured": True,
        "conversation_id": msg.conversation_id,
        "message_id": msg.message_id or msg.id,
        "status": msg.status.value if msg.status else None,
        "answer": "\n\n".join(answer) or description or error or "(Genie returned no text answer)",
        "description": description,
        "sql": sql,
        "table": table,
    }
