import json

from portfolio.utils.data_lakes.data_lake import DataLake


def build_key(context, index=None):
    # If a task has multiple outputs we need to name the keys appropriately.
    file_string = (
        f"{context['logical_date']}__{index}" if index else context["logical_date"]
    )

    identifiers = "_".join([f"{k}_{v}" for k, v in sorted(context["params"].items())])

    identifiers_path = f"{identifiers}/" if identifiers else ""

    return (
        f"{context['dag'].dag_id}/"
        f"{context['task'].task_id}/"
        f"{identifiers_path}"
        f"{file_string}"
    )


def save_to_data_lake(values, context, data_lake=None, index=None):

    data_lake = data_lake if data_lake is not None else DataLake()

    formatted_values = json.dumps(values)

    key = build_key(context, index=index)
    data_lake.put(key, formatted_values)
    return key
