import json
from datetime import datetime, timedelta
from functools import wraps

from sqlalchemy import Engine, inspect, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.schema import Column, MetaData, Table
from sqlalchemy.types import TEXT, DateTime, String


def cache_db(
    engine: Engine,
    schema,
    table,
    expire_duration=timedelta(days=1),
):
    _table = Table(
        table,
        MetaData(schema=schema),
        Column("func_name", String(), primary_key=True),
        Column("args", String(), primary_key=True),
        Column("kwargs", String(), primary_key=True),
        Column("result", TEXT()),
        Column("record_time", DateTime()),
    )

    with engine.connect() as conn:
        with conn.begin():
            if not inspect(conn).has_table(table, schema=schema):
                _table.create(conn)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            str_args = json.dumps(args)
            str_kwargs = json.dumps(kwargs)
            with engine.connect() as conn:
                result = conn.execute(
                    select(_table.c.result, _table.c.record_time)
                    .where(_table.c.func_name == str(func.__name__))
                    .where(_table.c.args == str_args)
                    .where(_table.c.kwargs == str_kwargs),
                ).first()
                if result and result[1] > datetime.now() - expire_duration:
                    return json.loads(result[0])

            result = func(*args, **kwargs)

            str_result = json.dumps(result)
            record_time = datetime.now()
            with engine.connect() as conn:
                insert_clause = (
                    insert(_table)
                    .values(
                        func_name=str(func.__name__),
                        args=str_args,
                        kwargs=str_kwargs,
                        result=str_result,
                        record_time=record_time,
                    )
                    .on_conflict_do_update(
                        index_elements=[
                            _table.c.func_name,
                            _table.c.args,
                            _table.c.kwargs,
                        ],
                        set_=dict(
                            result=str_result,
                            record_time=record_time,
                        ),
                    )
                )
                conn.execute(insert_clause)
                conn.commit()
            return result

        return wrapper

    return decorator
