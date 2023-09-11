from dwh.adapters.titanic_ds_adapter import BaseTitanicDataSetAdapter
from dwh.core.repository.passender_repository import BasePassengerRepository


# Dependency Inversion Principle
# Dependency Injection


class PassengerSaveJob:
    def __init__(self, downloader: BaseTitanicDataSetAdapter, repository: BasePassengerRepository):
        self.downloader = downloader
        self.repository = repository

    def execute(self):
        passengers = self.downloader.load()
        for passenger in passengers:
            self.repository.save(passenger)
