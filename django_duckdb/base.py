import duckdb

from django.db.backends.utils import CursorWrapper  # noqa
from django.db.backends.postgresql.base import (
    DatabaseWrapper as PostgresDatabaseWrapper,
)


class CustomCursorWrapper(CursorWrapper):
    def execute(self, sql, params=None):
        sql = f"USE memory; SET schema 'public'; {sql}"
        sql = sql.replace("%s", "?")
        self._query = None
        return self.cursor.execute(sql, params)

    def close(self):
        return self.cursor.close()


class DatabaseWrapper(PostgresDatabaseWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._duck_db_initialized = False

    def create_cursor(self, name=None):
        cursor = self._duckdb_connection.cursor()
        return CustomCursorWrapper(cursor, self)
    

    def _set_autocommit(self):
        return

    def get_new_connection(self, conn_params):
        self._duckdb_connection = duckdb.connect(":memory:ro")

        if not self._duck_db_initialized:
            dbname = conn_params["dbname"]
            user = conn_params["user"]
            password = conn_params["password"]
            host = conn_params["host"]
            port = conn_params["port"]

            self._duckdb_connection.sql(
                f"ATTACH 'dbname={dbname} user={user} password={password} host={host} port={port}' AS src (TYPE POSTGRES, SCHEMA 'public')"
            )
            self._duckdb_connection.sql("COPY FROM DATABASE src TO memory")

            self._duck_db_initialized = True

        print("new connection")
        return self._duckdb_connection
