import os
from os.path import join, dirname
import glob

import psycopg2
import prefect
from prefect.utilities.tasks import task

DATA_FOLDER = join(dirname(dirname(__file__)), 'output')


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
def prep_load(result, dsn, fieldnames, db_reset):
    logger = prefect.context.get("logger")

    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()

    logger.info("Database connection established")

    fields = infer_types(fieldnames)

    query = f"""
        CREATE TABLE IF NOT EXISTS temp_loading (
            {', '.join([f"{field} {fields[field]}" for field in fields])}
        );
    """
    cursor.execute(query)
    cursor.execute("TRUNCATE TABLE temp_loading")

    if db_reset:
            cursor.execute("TRUNCATE TABLE requests")

    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Database connection closed")


@task
def load_data(result, datasets, dsn, mode, target):
    logger = prefect.context.get("logger")
    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()

    list_of_files = []
    for item in datasets:
        file = join(DATA_FOLDER, f"download_dataset-{mode}-{item}-{prefect.context.today}.csv")
        if os.path.isfile(file):
            list_of_files.append(file)

    for file in list_of_files:

        with open(join(DATA_FOLDER, file), 'r') as f:
            try:
                cursor.copy_expert(
                    f"COPY temp_loading FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                    f
                )
                logger.info(f"Table 'temp_loading' successfully loaded from {os.path.basename(file)}")
            except (Exception, psycopg2.DatabaseError) as error:
                logger.info("Error: %s" % error)
                connection.rollback()
                cursor.close()

    connection.commit()

    cursor.close()
    connection.close()
    logger.info("Database connection closed")


@task
def complete_load(result, dsn, fieldnames, key, target, mode, db_reset):
    logger = prefect.context.get("logger")

    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()


    insert_query = f"""
        BEGIN;
        INSERT INTO {target} (
            {', '.join([f"{field}" for field in fieldnames])}
        )
        SELECT *
        FROM temp_loading
        ON CONFLICT (srnumber) 
        DO NOTHING;
        COMMIT;
    """

    update_query = f"""
        UPDATE {target}
        SET
            {', '.join([f"{field} = source.{field}" for field in fieldnames])}
        FROM (SELECT * FROM temp_loading) AS source
        WHERE {target}.{key} = source.{key};
    """

    refresh_view_query = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY service_requests;
    """

    cursor.execute("SELECT COUNT(*) FROM temp_loading")
    output = cursor.fetchone()
    
    logger.info(f"Insert/updating '{target}' table with {output[0]} new records")
    cursor.execute(insert_query)
    if db_reset is False or mode == "diff":
        logger.info(f"Updating {target} table with modified records")
        cursor.execute(update_query)

    connection.commit()

    logger.info(f"Table '{target}' was successfully updated")
    
    if db_reset:
        cursor.execute("TRUNCATE TABLE temp_loading")    

    cursor.execute(refresh_view_query)
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
