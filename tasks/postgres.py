import os
from os.path import join, dirname
import glob

import prefect
from prefect.utilities.tasks import task
import psycopg2


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
def prep_load(result, dsn, fieldnames):
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
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Database connection closed")


@task
def load_data(result, dsn):
    logger = prefect.context.get("logger")
    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()

    list_of_files = glob.glob(join(DATA_FOLDER, "download_dataset-diff-*.csv"))
    for file in list_of_files:

        with open(join(DATA_FOLDER, file), 'r') as f:
            try:
                cursor.copy_expert(
                    "COPY temp_loading FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                    f
                )
                logger.info(f"Temp table successfully loaded from {os.path.basename(file)}")
            except (Exception, psycopg2.DatabaseError) as error:
                logger.info("Error: %s" % error)
                connection.rollback()
                cursor.close()

    connection.commit()

    cursor.close()
    connection.close()
    logger.info("Database connection closed")


@task
def complete_load(result, dsn, fieldnames):
    logger = prefect.context.get("logger")

    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()


    insert_query = f"""
        INSERT INTO requests (
            {', '.join([f"{field}" for field in fieldnames])}
        )
        SELECT *
        FROM temp_loading
        ON CONFLICT (srnumber) 
        DO NOTHING
        RETURNING *;
    """

    update_query = f"""
        UPDATE requests
        SET
            {', '.join([f"{field} = source.{field}" for field in fieldnames])}
        FROM (SELECT * FROM temp_loading) AS source
        WHERE requests.srnumber = source.srnumber
        RETURNING *;
    """

    refresh_view_query = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY map;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vis;
        REFRESH MATERIALIZED VIEW CONCURRENTLY service_requests;
    """

    cursor.execute("SELECT COUNT(*) FROM temp_loading")
    output = cursor.fetchone()
    logger.info(f"Insert/updating requests table with {output[0]} new records")

    cursor.execute(insert_query)
    cursor.execute(update_query)

    connection.commit()

    logger.info("Requests table successfully updated")

    cursor.execute(refresh_view_query)
    cursor.execute("UPDATE metadata SET last_pulled = NOW()")
    connection.commit()
    logger.info("Views successfully refreshed")

    cursor.close()
    connection.close()
    logger.info("Database connection closed")


if __name__ == '__main__':

    dsn = os.getenv(
        "DATABASE_URL",
        "postgresql://311_user:311_pass@localhost:5433/311_db"
    )

    load_data(dsn)
