from typing import List

from dwh.core.domain.load_passengers_job import ITitanicPassengerRepository
from dwh.core.entities.passenger import Passenger


class TitanicPassengerAlchemyRepository(ITitanicPassengerRepository):
    pass

    def save(self, passenger: Passenger):
        pass

    def save_many(self, passengers: List[Passenger]):
        pass
