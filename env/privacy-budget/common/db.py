"""SQLite helper: thread-local connections, manual transaction control.

Atomicity model: writers execute `BEGIN IMMEDIATE`, which takes the database
write lock up front, so critical sections (check-then-act on budget balances)
are serialized. Combined with conditional UPDATEs (rowcount checked), two
concurrent requests can never both observe headroom and overspend.
"""
import os
import sqlite3
import threading


class DB:
    def __init__(self, path, schema):
        self.path = path
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        self._local = threading.local()
        self.conn().executescript(schema)
        self.conn().commit()

    def conn(self):
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA busy_timeout=30000")
            c.execute("PRAGMA foreign_keys=ON")
            self._local.conn = c
        return c
