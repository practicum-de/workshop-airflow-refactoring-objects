from dwh.core.entities.passenger import Passenger


class BasePassengerRepository:
    def save(self, passenger: Passenger):
        raise NotImplementedError


class PassengerRepository(BasePassengerRepository):
    def __init__(self, db):
        self.db = db

    def save(self, passenger: Passenger):
        # TODO. Implement this method
        pass
