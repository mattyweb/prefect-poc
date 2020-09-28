import os

import confuse
from prefect import Flow, Parameter
from prefect.engine.results import LocalResult
from prefect.utilities.edges import unmapped
from prefect.engine.executors import DaskExecutor, LocalExecutor
from prefect.tasks.secrets import EnvVarSecret

from tasks import postgres, socrata


"""
Configure the flow
Run the flow
Download data from Socrata
Insert downloaded data to TEMP table
Move data from TEMP to requests table
Update views
"""

with Flow('Loading Socrata Data to Postgres', result=LocalResult()) as flow:
    
    # parameters
    dataset_key_list = Parameter("dataset_key_list")
    fieldnames = Parameter("fieldnames")
    since = Parameter("since", required=False)
    mode = Parameter("mode", default="diff")
    domain = Parameter("domain")
    key = Parameter("key")
    target = Parameter("target")

    # get secrets from environment variables
    db_reset = EnvVarSecret("DB_RESET") or False
    token = EnvVarSecret("SOCRATA_TOKEN")
    dsn = EnvVarSecret("DSN")

    result = socrata.download_dataset.map(
        dataset_key=dataset_key_list,
        fieldnames=unmapped(fieldnames),
        since=unmapped(since),
        domain=unmapped(domain),
        token=unmapped(token)
    )
    result = postgres.prep_load(result, dsn, fieldnames, db_reset)
    result = postgres.load_data(result, dsn, mode, target)
    result = postgres.complete_load(result, dsn, fieldnames, key, target, mode, db_reset)


if __name__ == "__main__":

    config = confuse.Configuration('tutorial-prefect')
    config.set_file(os.path.join(os.path.dirname(__file__), "config.yaml"))
    
    mode = config['flow']['mode'].get() or 'diff'
    domain = config['flow']['domain'].get()
    dask = config['flow']['dask'].get(bool)
    dataset_pairs = config['flow']['datasets'].as_pairs()

    key = config['data']['key'].get()
    since = config['data']['since'].get()
    fields = config['data']['fields'].as_pairs()
    years = config['data']['years'].get()
    target = config['data']['target'].get()
    fieldnames = list(dict(fields).keys())

    # use only year datasets if in full mode otherwise use all w/since
    if mode == 'full':
        dataset_pairs = dict(dataset_pairs)
        dataset_key_list = [dataset_pairs[year] for year in years]
    else:
        dataset_key_list = list(dict(dataset_pairs).values())

    state = flow.run(
        dataset_key_list=dataset_key_list,
        fieldnames=fieldnames,
        key=key,
        mode=mode,
        target=target,
        since=since,
        domain=domain,
        executor=DaskExecutor() if dask else LocalExecutor()
    )
