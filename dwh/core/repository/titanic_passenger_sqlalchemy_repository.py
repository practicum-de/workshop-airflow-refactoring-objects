from datetime import datetime
from typing import List

from sqlalchemy import Boolean, Column, DateTime, Integer, Numeric, String

from dwh.core.domain.load_passengers_job import ITitanicPassengerRepository
from dwh.core.entities.passenger import Passenger
from dwh.core.repository.entity_base import EntityBase
from dwh.utils.session_builder import SessionBuilder


class PassengerEntity(EntityBase):
    """
    CREATE TABLE IF NOT EXISTS public.passengers_alchemy (
        id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        age NUMERIC NOT NULL,
        fare NUMERIC NOT NULL,
        name VARCHAR(255) NOT NULL UNIQUE,
        p_class INT NOT NULL,
        parents_children_aboard INT NOT NULL,
        gender INT NOT NULL,
        siblings_spouses_aboard INT NOT NULL,
        survived BOOLEAN NOT NULL,
        update_ts TIMESTAMP NOT NULL
    );
    """

    __tablename__ = "passengers_alchemy"
    __table_args__ = {"schema": "public"}

    id: int = Column("id", Integer, primary_key=True)  # type: ignore

    age: float = Column("age", Numeric, nullable=False)  # type: ignore
    fare: float = Column("fare", Numeric, nullable=False)  # type: ignore
    name: str = Column("name", String, nullable=False)  # type: ignore
    p_class: int = Column("p_class", Integer, nullable=False)  # type: ignore
    parents_children_aboard: int = Column("parents_children_aboard", Integer, nullable=False)  # type: ignore
    gender: int = Column("gender", Integer, nullable=False)  # type: ignore
    siblings_spouses_aboard: int = Column("siblings_spouses_aboard", Integer, nullable=False)  # type: ignore
    survived: bool = Column("survived", Boolean, nullable=False)  # type: ignore

    update_ts: datetime = Column("update_ts", DateTime)  # type: ignore


def to_entity(passenger: Passenger) -> PassengerEntity:
    return PassengerEntity(
        age=passenger.age,
        fare=passenger.fare,
        name=passenger.name,
        p_class=passenger.p_class,
        parents_children_aboard=passenger.parents_children_aboard,
        gender=passenger.gender,
        siblings_spouses_aboard=passenger.siblings_spouses_aboard,
        survived=passenger.survived,
        update_ts=datetime.utcnow(),
    )


class TitanicPassengerAlchemyRepository(ITitanicPassengerRepository):
    def __init__(self, session_builder: SessionBuilder) -> None:
        self.session_builder = session_builder

    def save(self, passenger: Passenger):
        ent_passenger = to_entity(passenger)
        with self.session_builder.session() as session:
            session.add(ent_passenger)

    def save_many(self, passengers: List[Passenger]):
        with self.session_builder.session() as session:
            ent_passengers = [to_entity(p) for p in passengers]
            session.add_all(ent_passengers)
