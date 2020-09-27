import os

import confuse
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

with Flow('Getting Socrata Data', result=LocalResult()) as flow:
    dataset_key_list = Parameter("dataset_key_list")
    fieldnames = Parameter("fieldnames")
    since = Parameter("since", required=False)
    domain = Parameter("domain")
    token = Parameter("token")
    dsn = Parameter("dsn")

    result = socrata.download_dataset.map(
        dataset_key=dataset_key_list,
        fieldnames=unmapped(fieldnames),
        since=unmapped(since),
        domain=unmapped(domain),
        token=unmapped(token)
    )
    result = postgres.prep_load(result, dsn, fieldnames)
    result = postgres.load_data(result, dsn)
    result = postgres.complete_load(result, dsn, fieldnames)


if __name__ == "__main__":

    config = confuse.Configuration('tutorial-prefect')
    config.set_file(os.path.join(os.path.dirname(__file__), "config.yaml"))
    
    domain = config['flow']['domain'].get()
    token = config['flow']['token'].get()
    dsn = config['flow']['dsn'].get()
    dask = config['flow']['dask'].get(bool)

    key = config['data']['key'].get()
    since = config['data']['since'].get()
    fields = config['data']['fields'].as_pairs()
    fieldnames = list(dict(fields).keys())
    dataset_pairs = config['data']['datasets'].as_pairs()
    dataset_key_list = list(dict(dataset_pairs).values())

    state = flow.run(
        dataset_key_list=dataset_key_list,
        fieldnames=fieldnames,
        since=since,
        domain=domain,
        token=token,
        dsn=dsn,
        executor=DaskExecutor() if dask else LocalExecutor()
    )
