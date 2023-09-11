from dwh.core.domain.load_passengers_job import ITitanicPassengerRepository
from dwh.core.entities.passenger import Passenger
from dwh.utils.postgres import PgConnect


class TitanicPassengerRepository(ITitanicPassengerRepository):
    def __init__(self, db_connection: PgConnect):
        self.db_connection = db_connection

    def save(self, passenger: Passenger):
        return super().save(passenger)

    def save_many(self, passengers: list[Passenger]):
        return super().save_many(passengers)
