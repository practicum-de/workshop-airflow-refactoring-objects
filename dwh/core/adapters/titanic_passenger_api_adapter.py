import csv
from typing import List

import requests

from dwh.core.domain.load_passengers_job import ITitanicPassengerDataAdapter
from dwh.core.entities.gender import Gender
from dwh.core.entities.passenger import Passenger


class TitanicPassengerApiAdapter(ITitanicPassengerDataAdapter):
    def __init__(self, url: str) -> None:
        self.url = url

    def download(self) -> List[Passenger]:
        def resolve_gender(sex: str) -> Gender:
            if sex == "male":
                return Gender.MALE

            if sex == "female":
                return Gender.FEMALE

            raise ValueError(f"sex {sex} is not recognized.")

        with requests.Session() as s:
            response = s.get(self.url)

        content = response.content.decode("utf-8")

        passenger_reader = csv.DictReader(content.splitlines(), delimiter=",")

        titanic_passengers = [
            Passenger(
                survived=bool(p["Survived"]),
                p_class=int(p["Pclass"]),
                name=p["Name"],
                gender=resolve_gender(p["Sex"]),
                age=float(p["Age"]),
                siblings_spouses_aboard=int(p["Siblings/Spouses Aboard"]),
                parents_children_aboard=int(p["Parents/Children Aboard"]),
                fare=float(p["Fare"]),
            )
            for p in passenger_reader
        ]

        return titanic_passengers
