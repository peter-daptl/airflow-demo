import json

from airflow.providers.mysql.hooks.mysql import MySqlHook
from iteration_utilities import unique_everseen

from portfolio.constants.connection_enum import Connection
from portfolio.utils.record import Record


class BaseLoader:

    FK_PRIORITY = []

    def __init__(self, *args, **kwargs):

        mysql_hook = MySqlHook(mysql_conn_id=Connection.CYCLING_DB.value)
        self.conn = mysql_hook.get_conn()
        self.conn.autocommit(True)
        self.cursor = self.conn.cursor()
        self.fks = {}
        self.fk_dict = {}

    def load(self, records):
        """
        Formats and executes data load for record in records.

        Args:
            records (list): List of dicts in "record" format
        """
        records = [json.loads(record) for record in records]
        for record in records:
            rec = Record(**record)
            self.unique_fks(rec.fks)

        for key, value in self.fks.items():
            self.fks[key] = list(unique_everseen(value))
        self.populate_fks()
        for record in records:
            for fk in record["fks"]:
                record["values"].update(
                    {
                        f"{fk['table']}_id": self.fk_dict[fk["table"]][
                            fk["values"]["name"]
                        ]
                    }
                )
                record["fks"] = []
            table = record["table"]
            cols = list(record["values"].keys())
            colstring = ", ".join(cols)
            vals = ["%({0})s".format(key) for key in list(record["values"].keys())]
            valstring = ", ".join(vals)
            dupes = dict(zip(cols, vals))
            dupestring = ",".join([f"{k} = {v}" for k, v in dupes.items()])

            sql = (
                f"INSERT INTO `{table}` "
                f"({colstring}) "
                f"VALUES ({valstring})"
                f"ON DUPLICATE KEY UPDATE {dupestring}"
            )

            self.cursor.execute(sql, record["values"])

    def unique_fks(self, fks):
        """
        Recursively adds fks to the fks list.

        Args:
            fks (list): List of fks
        """
        for fk in fks:
            self.unique_fks(fk.fks)
            self.fks.setdefault(fk.table, [])
            self.fks[fk.table].append(fk.__dict__())

    def populate_fks(self):
        """
        Populates the fks in the database.
        """
        for table in self.FK_PRIORITY:
            items = self.fks[table]
            for item in items:

                for fk in item["fks"]:
                    item["values"].update(
                        {
                            f"{fk['table']}_id": self.fk_dict[fk["table"]][
                                fk["values"]["name"]
                            ]
                        }
                    )
                    item["fks"] = []
                cols = list(item["values"].keys())
                colstring = ", ".join(cols)
                vals = ["%({0})s".format(key) for key in list(item["values"].keys())]
                valstring = ", ".join(vals)
                dupes = dict(zip(cols, vals))
                dupestring = ",".join([f"{k} = {v}" for k, v in dupes.items()])

                sql = (
                    f"INSERT INTO {table} "
                    f"({colstring}) "
                    f"VALUES ({valstring})"
                    f"ON DUPLICATE KEY UPDATE {dupestring}"
                )

                self.cursor.execute(sql, item["values"])
            self.populate_fk_dict(table)

    def populate_fk_dict(self, table):
        """
        Populates the fk_dict with the fks in the database. Used as a quick lookup for foreign keys.

        Args:
            table (str): The table to populate the fk_dict with
        """
        self.fk_dict[table] = {}
        sql = f"SELECT name, {table}_id FROM {table}"
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()

        for row in rows:
            self.fk_dict[table][row["name"]] = row[f"{table}_id"]
