import os
from os.path import join, dirname
import glob

import psycopg2
import prefect
from prefect.utilities.tasks import task

DATA_FOLDER = join(dirname(dirname(__file__)), 'output')
TEMP_TABLE = "temp_loading"

# TODO: make generic
def infer_types(fieldnames):
    fields = {}
    for field in fieldnames:
        if field[-4:] == 'date':
            fields[field] = "timestamp without time zone"
        elif field in {'nc', 'cd'}:
            fields[field] = "integer"
        elif field in {'latitude', 'longitude'}:
            fields[field] = "double precision"
        else:
            fields[field] = "character varying"
    return fields


@task
def prep_load(result):
    logger = prefect.context.get("logger")
    dsn = prefect.context.secrets["DSN"]

    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()

    logger.info("Database connection established")

    fields = prefect.config.data.fields
    db_reset = prefect.config.reset_db
    target = prefect.config.data.target

    query = f"""
        CREATE TABLE IF NOT EXISTS {TEMP_TABLE} (
            {', '.join([f"{field} {fields[field]}" for field in fields])}
        );
    """
    cursor.execute(query)
    cursor.execute(f"TRUNCATE TABLE {TEMP_TABLE}")

    if db_reset:
            cursor.execute(f"TRUNCATE TABLE {target}")

    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Database connection closed")


@task
def load_data(result, datasets):
    logger = prefect.context.get("logger")
    mode = prefect.config.mode
    dsn = prefect.context.secrets["DSN"]

    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()

    list_of_files = []
    for item in datasets:
        if mode == "full":
            file = join(DATA_FOLDER, f"{item}-{mode}.csv")
        else:
            file = join(DATA_FOLDER, f"{item}-{mode}-{prefect.context.today}.csv")

        if os.path.isfile(file):
            list_of_files.append(file)

    for file in list_of_files:
        
        with open(join(DATA_FOLDER, file), 'r') as f:
            try:
                cursor.copy_expert(
                    f"COPY {TEMP_TABLE} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                    f
                )
                logger.info(f"Table '{TEMP_TABLE}' successfully loaded from {os.path.basename(file)}")
            except (Exception, psycopg2.DatabaseError) as error:
                logger.info("Error: %s" % error)
                connection.rollback()
                cursor.close()

    connection.commit()

    cursor.close()
    connection.close()
    logger.info("Database connection closed")


@task
def complete_load(result):
    logger = prefect.context.get("logger")
    dsn = prefect.context.secrets["DSN"]

    mode = prefect.config.mode
    db_reset = prefect.config.reset_db
    fieldnames = list(prefect.config.data.fields.keys())
    key = prefect.config.data.key
    target = prefect.config.data.target

    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()

    insert_query = f"""
        BEGIN;
        INSERT INTO {target} (
            {', '.join([f"{field}" for field in fieldnames])}
        )
        SELECT *
        FROM {TEMP_TABLE}
        ON CONFLICT ({key}) 
        DO NOTHING;
        COMMIT;
    """

    update_query = f"""
        UPDATE {target}
        SET
            {', '.join([f"{field} = source.{field}" for field in fieldnames])}
        FROM (SELECT * FROM {TEMP_TABLE}) AS source
        WHERE {target}.{key} = source.{key};
    """

    # TODO make generic/configurable
    refresh_view_query = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY service_requests;
    """

    cursor.execute(f"SELECT COUNT(*) FROM {TEMP_TABLE}")
    output = cursor.fetchone()
    
    logger.info(f"Insert/updating '{target}' table with {output[0]} new records")
    cursor.execute(insert_query)
    if db_reset is False or mode == "diff":
        logger.info(f"Updating {target} table with modified records")
        cursor.execute(update_query)

    connection.commit()

    logger.info(f"Table '{target}' was successfully updated")
    
    if db_reset:
        cursor.execute(f"TRUNCATE TABLE {TEMP_TABLE}")    

    cursor.execute(refresh_view_query)
    # TODO make generic/configurable
    cursor.execute("UPDATE metadata SET last_pulled = NOW()")
    connection.commit()
    logger.info("Views successfully refreshed")

    # need to have autocommit set for VACUUM to work
    connection.autocommit = True
    cursor.execute("VACUUM FULL ANALYZE")
    logger.info("Database vacuumed and analyzed")

    cursor.close()
    connection.close()
    logger.info("Database connection closed")
