import csv

import requests

from dwh.core.entities.passenger import Passenger


class BaseTitanicDataSetAdapter:
    def load(self) -> list[Passenger]:
        raise NotImplementedError


class TitanicDataSetAdapter(BaseTitanicDataSetAdapter):
    def __init__(self, url: str):
        self.url = url

    def load(self) -> list[Passenger]:
        with requests.get(self.url, stream=True) as r:
            decoded_content = r.content.decode("utf-8")

            cr = csv.DictReader(decoded_content.splitlines(), delimiter=",")

            res = [
                Passenger(
                    name=row["Name"],
                    age=float(row["Age"]),
                    survived=bool(row["Survived"]),
                    p_class=int(row["Pclass"]),
                    sex=row["Sex"],
                    siblings_spouses_aboard=int(row["Siblings/Spouses Aboard"]),
                    parents_children_aboard=int(row["Parents/Children Aboard"]),
                    fare=float(row["Fare"]),
                )
                for row in cr
            ]
            return res
