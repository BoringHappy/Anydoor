from anydoor.dbs.postgres import Postgres


def SQLRunner(sql, db="Postgres", database="postgres", schema="public"):
    if db == "Postgres":
        Postgres(database=database, schema=schema).execute(sql)
