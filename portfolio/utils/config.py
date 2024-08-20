import logging

from airflow.hooks.base import BaseHook
from airflow.models import Variable


class Config:

    _airflow_variables = {}
    _airflow_connections = {}

    @classmethod
    def get_variable(cls, variable_name, *args, **kwargs):

        if variable_name not in Config._airflow_variables:
            logging.info(
                f"Retrieving variable {variable_name} from the airflow database"
            )
            Config._airflow_variables[variable_name] = Variable.get(
                variable_name, *args, **kwargs
            )

        return Config._airflow_variables[variable_name]

    @classmethod
    def get_connection(cls, connection_name):

        if connection_name not in Config._airflow_connections:
            logging.info(
                f"Retrieving connection {connection_name} from the airflow database"
            )
            Config._airflow_connections[connection_name] = BaseHook.get_connection(
                connection_name
            )

        return Config._airflow_connections[connection_name]
