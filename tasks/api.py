import requests

import prefect
from prefect.utilities.tasks import task


api_server = prefect.config.api.server

def reset_api_cache():
    api_server = prefect.config.api.server
    response = requests.post(f"{api_server}/status/reset-cache")
    value = response.json()["message"]
    return value


def get_last_updated():
    api_server = prefect.config.api.server
    response = requests.get(f"{api_server}/status/api")
    value = response.json()["lastPulled"]
    return value


def calculate_since():
    file_since = prefect.config.data.since
    api_since = get_last_updated()
    return max(file_since, api_since)


def post_to_api(task, old_state, new_state):
    # if new_state.is_finished():
    msg = "Task {0} finished in state {1}".format(task, new_state)

    logger = prefect.context.get("logger")
    logger.info(msg)

    # replace URL with your Slack webhook URL
    requests.post(f"{api_server}/status/loading-state", json={"text": msg})

    return new_state
