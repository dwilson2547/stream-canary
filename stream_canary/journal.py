"""Sqlite journal of mirrored inputs: prod offset, join key, payload hash."""

import hashlib
import json
import sqlite3
import time


class Journal:
    def __init__(self, path: str, join_key: str):
        self.path = path
        self.join_key = join_key
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mirrored (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prod_offset INTEGER NOT NULL,
                join_value TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                mirrored_at_ms INTEGER NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_join ON mirrored(join_value)"
        )
        self.conn.commit()

    def record(self, prod_offset: int, value: bytes | None) -> None:
        """Record one mirrored message. `value` is the raw Kafka payload (JSON)."""
        if value is None:
            raise ValueError("mirrored message has no payload to derive join key from")
        payload = json.loads(value)
        if self.join_key not in payload:
            raise KeyError(
                f"join key {self.join_key!r} not present in mirrored payload "
                f"(keys: {sorted(payload)})"
            )
        digest = hashlib.sha256(value).hexdigest()
        self.conn.execute(
            "INSERT OR REPLACE INTO mirrored (prod_offset, join_value, payload_sha256, mirrored_at_ms)"
            " VALUES (?, ?, ?, ?)",
            (prod_offset, str(payload[self.join_key]), digest, int(time.time() * 1000)),
        )
        self.conn.commit()

    def join_values(self) -> list[str]:
        rows = self.conn.execute("SELECT join_value FROM mirrored ORDER BY prod_offset").fetchall()
        return [r[0] for r in rows]

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM mirrored").fetchone()[0]

    def close(self) -> None:
        self.conn.close()
