from datetime import date

import requests
from airflow.operators.python import get_current_context

from portfolio.constants.variable_enum import Variable
from portfolio.extractors.base_extractor import BaseExtractor
from portfolio.utils.config import Config
from portfolio.utils.exceptions import HTTPDataLoadException


class UCIRankingsExtractor(BaseExtractor):

    def extract(self):
        link = Config.get_variable(Variable.FIRST_CYCLING.value)
        context = get_current_context()
        dt = context["execution_date"]
        year = dt.isocalendar().year
        week = dt.isocalendar().week - 1
        if week == 0:
            dt = date(year=year - 1, month=12, day=31)
            year = dt.isocalendar().year
            week = dt.isocalendar().week - 1

        try:
            r = requests.get(
                link.format(
                    year=year,
                    week=week,
                )
            )
            r.raise_for_status()
            data = str(r.content.decode("utf8"))
        except requests.exceptions.HTTPError as err:
            raise HTTPDataLoadException(err)
        return data
