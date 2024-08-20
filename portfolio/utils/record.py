import json

from portfolio.utils.exceptions import RecordValidationException


class Record:

    def __init__(self, *, table="", pk="", fks=[], values={}):
        if not table.strip():
            raise RecordValidationException("Table must be defined")

        if not pk.strip():
            raise RecordValidationException("pk must be defined")

        if not pk.strip():
            raise RecordValidationException("pk must be defined")

        if not values:
            raise RecordValidationException("A record must have some values")

        self.table = table
        self.pk = pk
        self.fks = []
        for fk in fks:
            if not isinstance(fk, Record):
                fk = Record(**fk)
            self.fks.append(fk)
        self.values = values

    def __dict__(self):
        return {
            "table": self.table,
            "pk": self.pk,
            "fks": [fk.__dict__() for fk in self.fks],
            "values": self.values,
        }

    def toJSON(self):
        return json.dumps(self.__dict__())
