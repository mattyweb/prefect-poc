import os

import prefect
from prefect import Flow, Parameter
from prefect.engine.results import LocalResult
from prefect.utilities.edges import unmapped
from prefect.engine.executors import DaskExecutor, LocalExecutor

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
        result=LocalResult(
            dir=os.path.join(os.path.dirname(__file__), "results"),
            validate_dir=True
            )
    ) as flow:
    
    # parameters
    datasets = Parameter("datasets")
    since = Parameter("since")

    result = socrata.download_dataset.map(
        dataset=datasets,
        since=unmapped(since)
    )
    result = postgres.prep_load(result)
    result = postgres.load_data(result, datasets)
    result = postgres.complete_load(result)


if __name__ == "__main__":

    dask = prefect.config.dask
    mode = prefect.config.mode or 'diff'
    all_datasets = dict(prefect.config.socrata.datasets)
    years = list(prefect.config.data.years)
    since = prefect.config.data.since

    # use only year datasets if in full mode otherwise use all w/since
    if mode == 'full':
        run_datasets = dict((k, all_datasets[k]) for k in years)
    else:
        run_datasets = all_datasets

    state = flow.run(
        datasets=list(run_datasets.values()),
        since=since,
        executor=DaskExecutor() if dask else LocalExecutor()
    )
