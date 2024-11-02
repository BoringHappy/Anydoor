from functools import wraps
import json
from sqlalchemy.types import String, DateTime
from sqlalchemy import select
from datetime import datetime, timedelta
from sqlalchemy.dialects.postgresql import insert
from ..dbs.postgres import Postgres


def cache_db(
    conn: Postgres,
    schema,
    table,
    set_pk=True,
    expire_duration=timedelta(days=1),
    return_cache=True,
    cache_condition=None,
):
    conn.ensure_table(
        table=table,
        schema=schema,
        dtype={
            "func_name": String(),
            "args": String(),
            "kwargs": String(),
            "result": String(),
            "record_time": DateTime(),
        },
        primary_keys=["func_name", "args", "kwargs"] if set_pk else None,
    )

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _table = conn.get_table(
                table=table,
                schema=schema,
            )
            str_args = json.dumps(args)
            str_kwargs = json.dumps(kwargs)
            if return_cache:
                result = conn.execute(
                    select(_table.c.result, _table.c.record_time)
                    .where(_table.c.func_name == str(func.__name__))
                    .where(_table.c.args == str_args)
                    .where(_table.c.kwargs == str_kwargs),
                    return_pandas=False,
                ).first()
                if result:
                    if result[1] > datetime.now() - expire_duration:
                        return json.loads(result[0])

            result = func(*args, **kwargs)

            str_result = json.dumps(result)
            record_time = datetime.now()
            if cache_condition is None or cache_condition(str_result):
                insert_clause = insert(_table).values(
                    func_name=str(func.__name__),
                    args=str_args,
                    kwargs=str_kwargs,
                    result=str_result,
                    record_time=record_time,
                )
                if set_pk:
                    insert_clause = insert_clause.on_conflict_do_update(
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

                conn.execute(insert_clause)
            return result

        return wrapper

    return decorator
