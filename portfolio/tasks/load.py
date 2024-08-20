import logging

from airflow.decorators import task
from airflow.operators.python import get_current_context

from portfolio.loaders.base_loader import BaseLoader
from portfolio.utils.data_lakes.data_lake import DataLake
from portfolio.utils.factory import Factory


@task
def load(transform_key, prefix=None, params=None):

    context = get_current_context()

    logging.info(
        f"Executing {context['task'].task_id} task "
        f"of DAG {context['dag'].dag_id} with run Id of {context['run_id']} "
        f"and logical date of {context['logical_date']}"
    )

    loader = Factory.create_subclass(
        BaseLoader,
        prefix=prefix if prefix else context["dag"].dag_id,
        **(params if params else context["params"]),
        use_base=True,
    )

    logging.info(
        f"{loader.__class__.__name__} created for {context['task'].task_id} task"
    )

    data_lake = DataLake()
    transform_values = data_lake.get(transform_key)
    loader.load(transform_values)

    logging.info(f"{loader.__class__.__name__} has completed loading")
