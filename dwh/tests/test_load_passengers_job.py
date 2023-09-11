from dwh.core.domain.load_passengers_job import ITitanicPassengerDataAdapter, ITitanicPassengerRepository, LoadPassengersJob
from dwh.core.entities.gender import Gender
from dwh.core.entities.passenger import Passenger


class MockTitanicPassengerDataAdapter(ITitanicPassengerDataAdapter):
    def __init__(self, passengers: list[Passenger]) -> None:
        self.passengers = passengers

    def download(self) -> list[Passenger]:
        return self.passengers


class MockTitanicPassengerRepository(ITitanicPassengerRepository):
    def __init__(self, passengers: list[Passenger] = []) -> None:
        self._passengers = passengers

    def save(self, passenger: Passenger):
        self._passengers.append(passenger)

    def save_many(self, passengers: list[Passenger]):
        self._passengers.extend(passengers)

    def list_all(self) -> list[Passenger]:
        return self._passengers


class Test_LoadPassengersJob:
    def test_execute(self):
        input_data = [
            Passenger(
                name="Mr. Owen Harris Braund",
                age=22.0,
                survived=True,
                p_class=3,
                fare=7.25,
                parents_children_aboard=0,
                siblings_spouses_aboard=1,
                gender=Gender.MALE,
            )
        ]

        adapter = MockTitanicPassengerDataAdapter(input_data)
        repository = MockTitanicPassengerRepository()

        job = LoadPassengersJob(adapter, repository)
        job.execute()

        repository_passengers = repository.list_all()
        assert repository_passengers is not None
        assert len(repository_passengers) == 1

        check_passenger = next((p for p in repository_passengers if p.name == "Mr. Owen Harris Braund"), None)
        assert check_passenger is not None
        assert check_passenger.age == 22.0
        assert check_passenger.survived
        assert check_passenger.p_class == 3
        assert check_passenger.fare == 7.25
        assert check_passenger.parents_children_aboard == 0
        assert check_passenger.siblings_spouses_aboard == 1
        assert check_passenger.gender == Gender.MALE
