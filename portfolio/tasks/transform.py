import logging

from airflow.decorators import task
from airflow.operators.python import get_current_context

from portfolio.transformers.base_transformer import BaseTransformer
from portfolio.utils.data_lakes.data_lake import DataLake
from portfolio.utils.factory import Factory
from portfolio.utils.task_utils import save_to_data_lake


@task
def transform(extract_key=None, *, prefix=None, params=None):
    context = get_current_context()
    logging.info(
        f"Executing {context['task'].task_id} task "
        f"of DAG {context['dag'].dag_id} with run Id of {context['run_id']} "
        f"and logical date of {context['logical_date']}"
    )

    transformer = Factory.create_subclass(
        BaseTransformer,
        prefix=prefix + "transformer" if prefix else context["dag"].dag_id,
        **context["params"],
    )

    logging.info(
        f"{transformer.__class__.__name__} created for "
        f"{context['task'].task_id} task"
    )

    data_lake = DataLake()
    extract_values = data_lake.get(extract_key)
    values = transformer.transform(extract_values)

    if getattr(transformer, "MULTIPLE_OUTPUTS", False):
        keys_dict = {}
        for index, value in enumerate(values):
            index = str(index + 1)
            key = save_to_data_lake(value, context, index=index)
            keys_dict[index] = key
        return keys_dict

    logging.info(f"{transformer.__class__.__name__} has completed transform")
    logging.info(f"Transform values: {values}")

    key = save_to_data_lake(values, context, data_lake=data_lake)

    return key
