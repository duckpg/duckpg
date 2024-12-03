class ReadWriteRouter:
    def db_for_read(self, model, **hints):
        return "duckdb"

    def db_for_write(self, model, **hints):
        return "default"

    def allow_relation(self, obj1, obj2, **hints):
        return obj1._state.db == obj2._state.db

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return db == "default"
