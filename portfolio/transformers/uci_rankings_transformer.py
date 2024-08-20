from datetime import date

from airflow.operators.python import get_current_context
from bs4 import BeautifulSoup

from portfolio.transformers.base_transformer import BaseTransformer
from portfolio.utils.record import Record


class UCIRankingsTransformer(BaseTransformer):

    def transform(self, feed):
        records = []
        context = get_current_context()
        dt = context["execution_date"]
        year = dt.isocalendar().year
        week = dt.isocalendar().week - 1
        if week == 0:
            dt = date(year=year - 1, month=12, day=31)
            year = dt.isocalendar().year
            week = dt.isocalendar().week - 1
        soup = BeautifulSoup(feed, "html.parser")
        table = soup.find("table", {"class": "tablesorter sort"})
        tbody = table.find("tbody")
        rows = tbody.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            team = Record(
                table="team",
                pk="team_id",
                values={"name": cells[4].find("a").text.strip()},
            )
            country = Record(
                table="country",
                pk="country_id",
                values={"name": cells[3].find("a").text.strip()},
            )
            rider = Record(
                table="rider",
                pk="rider_id",
                fks=[team, country],
                values={"name": cells[2].find("a").text},
            )

            points = cells[5]
            for match in points.find_all("span"):
                match.decompose()

            ranking = Record(
                table="rank",
                pk="rank_id",
                fks=[rider],
                values={
                    "ranking": cells[0].text.strip(),
                    "points": int(points.text.strip().replace(".", "")),
                    "season": year,
                    "week": week,
                },
            )
            records.append(ranking.toJSON())
        return records
