import os
import sys

from snowflake.ml.jobs import remote
from snowflake.snowpark import Session

# Config

SRC_TABLE        = sys.argv[1] if len(sys.argv) > 1 else 'PY_LOGS_DEMO.PUBLIC.SCD1_SOURCE'
TGT_TABLE        = sys.argv[2] if len(sys.argv) > 2 else 'PY_LOGS_DEMO.PUBLIC.SCD1_TARGET'
KEY_COLS_CSV     = sys.argv[3] if len(sys.argv) > 3 else 'ID'
TRACKED_COLS_CSV = sys.argv[4] if len(sys.argv) > 4 else 'NAME,STATUS'

COMPUTE_POOL = 'PY_LOGS_DEMO_POOL'
STAGE        = '@PY_LOGS_DEMO.PUBLIC.CODE_STAGE'

# Session

connection_name = os.environ.get('SNOW_CONNECTION', 'default')
session = Session.builder.config('connection_name', connection_name).create()
session.use_database('PY_LOGS_DEMO')
session.use_schema('PUBLIC')

# Remote function

@remote(session=session, compute_pool=COMPUTE_POOL, stage_name=STAGE)
def scd1_merge(src_table: str, tgt_table: str, key_cols_csv: str, tracked_cols_csv: str):
    import logging
    import sys
    import traceback
    from snowflake.snowpark import Session

    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s',
                        stream=sys.stdout, force=True)
    logger = logging.getLogger('scd1_job')
    # logger.setLevel(logging.INFO)

    session = Session.builder.getOrCreate()
    key_cols     = [c.strip() for c in key_cols_csv.split(',')]
    tracked_cols = [c.strip() for c in tracked_cols_csv.split(',')]

    try:
        current_user = session.sql('SELECT CURRENT_USER() AS u').collect()[0]['U']
        logger.info(
            f'User {current_user} started SCD1 merge | '
            f'src={src_table} tgt={tgt_table} | '
            f'key_cols={key_cols} tracked_cols={tracked_cols}'
        )
        logger.warning('This is how warning looks')

        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {src_table} (
                ID           INT,
                NAME         VARCHAR,
                STATUS       VARCHAR,
                LAST_UPDATED TIMESTAMP
            )
        """).collect()

        session.sql(f'DELETE FROM {src_table}').collect()

        session.sql(f"""
            INSERT INTO {src_table} VALUES
                (1, 'Alice',   'active',   CURRENT_TIMESTAMP()),
                (2, 'Bob',     'inactive', CURRENT_TIMESTAMP()),
                (3, 'Charlie', 'active',   CURRENT_TIMESTAMP())
        """).collect()

        logger.info(f'Source table {src_table} created with 3 rows (LAST_UPDATED column is untracked)')

        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {tgt_table} (
                ID     INT,
                NAME   VARCHAR,
                STATUS VARCHAR
            )
        """).collect()

        src_cols = {f.name.upper() for f in session.table(src_table).schema.fields}
        tgt_cols = {f.name.upper() for f in session.table(tgt_table).schema.fields}

        missing_key_cols = [c for c in key_cols if c.upper() not in src_cols]
        if missing_key_cols:
            common_cols = sorted(src_cols & tgt_cols)
            msg = (
                f'Key columns not found in source table {src_table}: {missing_key_cols}. '
                f'Columns present in both source and target: {common_cols}'
            )
            logger.error(msg)
            raise ValueError(msg)

        src_count = session.sql(f'SELECT COUNT(*) AS cnt FROM {src_table}').collect()[0]['CNT']
        logger.info(f'Source table {src_table} has {src_count} rows')

        tgt_count = session.sql(f'SELECT COUNT(*) AS cnt FROM {tgt_table}').collect()[0]['CNT']
        logger.info(f'Target table {tgt_table} has {tgt_count} rows before merge')

        join_clause   = ' AND '.join(f't.{c} = s.{c}' for c in key_cols)
        update_clause = ', '.join(f't.{c} = s.{c}' for c in tracked_cols)
        all_cols      = key_cols + tracked_cols
        insert_cols   = ', '.join(all_cols)
        insert_vals   = ', '.join(f's.{c}' for c in all_cols)

        merge_sql = f"""
            MERGE INTO {tgt_table} AS t
            USING {src_table} AS s
            ON {join_clause}
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        result = session.sql(merge_sql).collect()[0]
        logger.info(
            f"MERGE complete: {result['number of rows inserted']} inserted, "
            f"{result['number of rows updated']} updated"
        )

    except Exception as e:
        logger.error(f'SCD1 merge failed for {tgt_table}: {e}\n{traceback.format_exc(1)}')
        raise

    logger.info('Execution finished successfully')


# Submit

if __name__ == '__main__':
    import time

    job = scd1_merge(SRC_TABLE, TGT_TABLE, KEY_COLS_CSV, TRACKED_COLS_CSV)
    print(f'Job submitted: {job.id}')

    seen = 0
    while job.status in ('PENDING', 'RUNNING'):
        lines = job.get_logs(as_list=True)
        for line in lines[seen:]:
            print(line)
        seen = len(lines)
        time.sleep(2)

    # Print any remaining lines after completion
    lines = job.get_logs(as_list=True)
    for line in lines[seen:]:
        print(line)

    print(f'\nJob finished: {job.status}')
    session.close()
