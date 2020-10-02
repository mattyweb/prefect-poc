import os
from tasks.postgres import complete_load, load_data, prep_load

import prefect
from prefect import Flow, Parameter
from prefect.engine.results import LocalResult
from prefect.utilities.edges import unmapped
from prefect.engine.executors import LocalDaskExecutor, LocalExecutor

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
            ),
        state_handlers=[postgres.log_to_database]
    ) as flow:
    
    datasets = Parameter("datasets")
    since = postgres.get_last_updated()
    download = socrata.download_dataset.map(
        dataset=datasets,
        since=unmapped(since)
    )
    prep = postgres.prep_load()
    load = postgres.load_data(datasets)
    complete = postgres.complete_load()

    flow.add_edge(upstream_task=since, downstream_task=download)
    flow.add_edge(upstream_task=download, downstream_task=prep)
    flow.add_edge(upstream_task=prep, downstream_task=load)
    flow.add_edge(upstream_task=load, downstream_task=complete)


if __name__ == "__main__":

    dask = prefect.config.dask
    mode = prefect.config.mode or 'diff'
    all_datasets = dict(prefect.config.socrata.datasets)
    years = list(prefect.config.data.years)

    # use only year datasets if in full mode otherwise use all w/since
    if mode == 'full':
        run_datasets = dict((k, all_datasets[k]) for k in years)
    else:
        run_datasets = all_datasets

    state = flow.run(
        datasets=list(run_datasets.values()),
        executor=LocalDaskExecutor() if dask else LocalExecutor()
    )
