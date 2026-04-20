import logging
import traceback

logger = logging.getLogger("scd1_proc")


def run_scd1(session, src_table: str, tgt_table: str, key_cols: list, tracked_cols: list):
    """Execute a SCD Type 1 MERGE from src_table into tgt_table."""

    src_cols = {f.name.upper() for f in session.table(src_table).schema.fields}
    tgt_cols = {f.name.upper() for f in session.table(tgt_table).schema.fields}

    missing_key_cols = [c for c in key_cols if c.upper() not in src_cols]
    if missing_key_cols:
        common_cols = sorted(src_cols & tgt_cols)
        msg = f"""Key columns not found in source table {src_table}: {missing_key_cols}.
Columns present in both source and target: {common_cols}"""
        logger.error(
            msg,
            extra = {'error_type': 'INVALID_SCD1_KEY', 'input_cols': key_cols, 'invalid_cols': missing_key_cols}
        )
        raise ValueError(msg)

    src_count = session.sql(f"SELECT COUNT(*) AS cnt FROM {src_table}").collect()[0]["CNT"]
    logger.info(f"Source table {src_table} has {src_count} rows")

    tgt_count = session.sql(f"SELECT COUNT(*) AS cnt FROM {tgt_table}").collect()[0]["CNT"]
    logger.info(f"Target table {tgt_table} has {tgt_count} rows before merge")

    join_clause = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
    update_clause = ", ".join(f"t.{c} = s.{c}" for c in tracked_cols)
    all_cols = key_cols + tracked_cols
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"s.{c}" for c in all_cols)

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
    rows_inserted = result["number of rows inserted"]
    rows_updated = result["number of rows updated"]
    logger.info(f"MERGE complete: {rows_inserted} inserted, {rows_updated} updated")


def main(session, src_table: str, tgt_table: str, key_cols_csv: str, tracked_cols_csv: str):
    try:
        current_user = session.sql("SELECT CURRENT_USER() AS u").collect()[0]["U"]
        key_cols = [c.strip() for c in key_cols_csv.split(",")]
        tracked_cols = [c.strip() for c in tracked_cols_csv.split(",")]

        logger.info(
            f"User {current_user} started SCD1 merge | "
            f"src={src_table} tgt={tgt_table} | "
            f"key_cols={key_cols} tracked_cols={tracked_cols}"
        )

        # Create fake source data.
        # LAST_UPDATED is intentionally untracked — it exists in source but is not
        # listed in tracked_cols, so SCD1 will not propagate it to the target table.
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {src_table} (
                ID           INT,
                NAME         VARCHAR,
                STATUS       VARCHAR,
                LAST_UPDATED TIMESTAMP
            )
        """).collect()

        session.sql(f"DELETE FROM {src_table};").collect()

        session.sql(f"""
            INSERT INTO {src_table} VALUES
                (1, 'Alice',   'active',   CURRENT_TIMESTAMP()),
                (2, 'Bob',     'inactive', CURRENT_TIMESTAMP()),
                (3, 'Charlie', 'active',   CURRENT_TIMESTAMP())
        """).collect()

        logger.info(f"Source table {src_table} created with 3 rows (LAST_UPDATED column is untracked)")

        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {tgt_table} (
                ID     INT,
                NAME   VARCHAR,
                STATUS VARCHAR
            )
        """).collect()

        run_scd1(session, src_table, tgt_table, key_cols, tracked_cols)
    except Exception as e:
        logger.error(f"SCD1 merge failed for {tgt_table}: {e}\n{traceback.format_exc(1)}")
        raise e
    logger.info(f"Execution finished successfully")

    return "done"
