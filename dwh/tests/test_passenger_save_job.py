from dwh.adapters.titanic_ds_adapter import BaseTitanicDataSetAdapter
from dwh.core.domain.passenger_save_job import PassengerSaveJob
from dwh.core.entities.passenger import Passenger
from dwh.core.repository.passender_repository import BasePassengerRepository


class MockPassengerAdapter(BaseTitanicDataSetAdapter):
    def __init__(self) -> None:
        self.__passengers = [
            Passenger(
                name="Mr. Owen Harris Braund",
                age=22.0,
                survived=True,
                p_class=3,
                sex="male",
                siblings_spouses_aboard=1,
                parents_children_aboard=0,
                fare=7.25,
            )
        ]

    def download(self) -> list[Passenger]:
        return self.__passengers


class MockPassengerRepository(BasePassengerRepository):
    def __init__(self) -> None:
        self._passengers = []

    def save(self, passenger: Passenger):
        self._passengers.append(passenger)


class Test_PassengerSaveJob:
    def test_execute(self):
        adapter = MockPassengerAdapter()
        repository = MockPassengerRepository()

        job = PassengerSaveJob(adapter, repository)
        job.execute()

        assert repository._passengers is not None
        assert len(repository._passengers) == 1
