from datetime import datetime

from airflow.decorators import dag

from portfolio.tasks.extract import extract
from portfolio.tasks.load import load
from portfolio.tasks.transform import transform

target = "UCIRankings"


@dag(
    default_args={
        "owner": "airflow",
        "retries": 0,
        "depends_on_past": False,
        "on_failure_callback": None,
    },
    schedule_interval="0 12 * * WED",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
)
def uci_rankings():
    extracted = extract(prefix=target)
    transformed = transform(extracted, prefix=target)
    load(transformed, prefix=target)


uci_rankings = uci_rankings()
