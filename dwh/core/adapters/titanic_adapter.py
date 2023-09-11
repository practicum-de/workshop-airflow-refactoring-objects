import csv

import requests

from dwh.core.entities.passenger import Passenger


class TitanicApiAdapter:
    def __init__(self, url: str) -> None:
        self.url = url

    def download(self) -> list[Passenger]:
        with requests.Session() as s:
            download = s.get(self.url)

        decoded_content = download.content.decode("utf-8")

        passenger_reader = csv.DictReader(decoded_content.splitlines(), delimiter=",")
        titanic_passengers = [
            Passenger(
                survived=bool(p["Survived"]),
                p_class=int(p["Pclass"]),
                name=p["Name"],
                sex=p["Sex"],
                age=float(p["Age"]),
                siblings_spouses_aboard=int(p["Siblings/Spouses Aboard"]),
                parents_children_aboard=int(p["Parents/Children Aboard"]),
                fare=float(p["Fare"]),
            )
            for p in passenger_reader
        ]

        return titanic_passengers
