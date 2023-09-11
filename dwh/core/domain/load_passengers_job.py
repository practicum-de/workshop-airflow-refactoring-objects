from abc import ABC, abstractmethod

from dwh.core.entities.passenger import Passenger


class ITitanicPassengerDataAdapter(ABC):
    @abstractmethod
    def download(self) -> list[Passenger]:
        raise NotImplementedError


class ITitanicPassengerRepository(ABC):
    @abstractmethod
    def save(self, passenger: Passenger):
        raise NotImplementedError

    @abstractmethod
    def save_many(self, passengers: list[Passenger]):
        raise NotImplementedError


class LoadPassengersJob:
    def __init__(self, adapter: ITitanicPassengerDataAdapter, repository: ITitanicPassengerRepository):
        self.adapter = adapter
        self.repository = repository

    def execute(self):
        passengers = self.adapter.download()
        self.repository.save_many(passengers)
