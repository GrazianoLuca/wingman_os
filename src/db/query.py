from __future__ import annotations

import pandas as pd
from pathlib import Path
import duckdb

from src.db.database import get_db_connection

# src/db/database.py -> parent(db) -> parent(src) -> parent(project root)
DB_PATH = Path(__file__).resolve().parent / "test.db"

def get_inactive_users(days: int = 3) -> list[str]:


    """
    Returns user_ids from chat/gift who have no events across any table in the last x days.
    """

    query = f"""
    WITH all_events AS (
        SELECT user_id, timestamp FROM chat
        UNION ALL
        SELECT user_id, timestamp FROM gift
    ),
    target_users AS (
        -- Target cohort: distinct users present in chat or gift
        SELECT user_id FROM chat
        UNION
        SELECT user_id FROM gift
    ),
    user_activity AS (
        SELECT 
            user_id,
            MAX(CAST(timestamp AS TIMESTAMP)) AS last_event_at
        FROM all_events
        GROUP BY user_id
    )
    SELECT tu.user_id
    FROM target_users tu
    JOIN user_activity ua ON tu.user_id = ua.user_id
    WHERE ua.last_event_at < CURRENT_TIMESTAMP - INTERVAL {days} DAYS;
    """
    
    with get_db_connection() as conn:
        res = conn.execute(query).fetchall()
        out = [row[0] for row in res]
        print(out)
        return out


def check_data_in_table(table: str) -> pd.DataFrame:
    query = f"""
        SELECT user_id, COUNT(comment) AS comment_count
        FROM {table}
        GROUP BY user_id
    """

    with get_db_connection() as conn:
            result = conn.execute(query).df()
            print(result)
            return result


if __name__ == "__main__":
    get_inactive_users()
    check_data_in_table('chat')