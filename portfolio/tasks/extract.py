import logging

from airflow.decorators import task
from airflow.operators.python import get_current_context

from portfolio.extractors.base_extractor import BaseExtractor
from portfolio.utils.data_lakes.data_lake import DataLake
from portfolio.utils.factory import Factory
from portfolio.utils.task_utils import save_to_data_lake


@task
def extract(prev_key=None, *, prefix=None, params=None):

    context = get_current_context()
    logging.info(
        f"Executing {context['task'].task_id} task "
        f"of DAG {context['dag'].dag_id} with run Id of {context['run_id']} "
        f"and logical date of {context['logical_date']}"
    )

    extractor = Factory.create_subclass(
        BaseExtractor,
        prefix=prefix if prefix else context["dag"].dag_id,
        default=BaseExtractor,
        **(params if params else context["params"]),
    )

    logging.info(
        f"{extractor.__class__.__name__} created for {context['task'].task_id} task"
    )

    if prev_key:
        data_lake = DataLake()
        prev_values = data_lake.get(prev_key)
        values = extractor.extract(prev_values)
    else:
        values = extractor.extract()

    logging.info(f"{extractor.__class__.__name__} has completed extraction")
    logging.info(f"Extracted values: {values}")

    if getattr(extractor, "MULTIPLE_OUTPUTS", False):
        keys_dict = {}
        for index, value in enumerate(values):
            index = str(index + 1)
            key = save_to_data_lake(value, context, index=index)
            keys_dict[index] = key

        return keys_dict

    key = save_to_data_lake(values, context)
    return key
