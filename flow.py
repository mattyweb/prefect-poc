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

with Flow(
    'Loading Socrata Data to Postgres',
    result=LocalResult(dir=os.path.join(os.path.dirname(__file__), "results"), validate_dir=True)
    ) as flow:
    
    # parameters
    datasets = Parameter("datasets")
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
        dataset=datasets,
        mode=unmapped(mode),
        fieldnames=unmapped(fieldnames),
        since=unmapped(since),
        domain=unmapped(domain),
        token=unmapped(token)
    )
    result = postgres.prep_load(result, dsn, fieldnames, db_reset)
    result = postgres.load_data(result, datasets, dsn, mode, target)
    result = postgres.complete_load(result, dsn, fieldnames, key, target, mode, db_reset)


if __name__ == "__main__":

    config = confuse.Configuration('tutorial-prefect')
    config.set_file(os.path.join(os.path.dirname(__file__), "config.yaml"))
    
    mode = config['flow']['mode'].get() or 'diff'
    domain = config['flow']['domain'].get()
    dask = config['flow']['dask'].get(bool)
    all_datasets = dict(config['flow']['datasets'].as_pairs())

    key = config['data']['key'].get()
    since = config['data']['since'].get()
    fields = config['data']['fields'].as_pairs()
    years = config['data']['years'].get()
    target = config['data']['target'].get()
    fieldnames = list(dict(fields).keys())

    # use only year datasets if in full mode otherwise use all w/since
    if mode == 'full':
        run_datasets = dict((k, all_datasets[k]) for k in years)
    else:
        run_datasets = all_datasets

    state = flow.run(
        datasets=list(run_datasets.values()),
        fieldnames=fieldnames,
        key=key,
        mode=mode,
        target=target,
        since=since,
        domain=domain,
        executor=DaskExecutor() if dask else LocalExecutor()
    )
