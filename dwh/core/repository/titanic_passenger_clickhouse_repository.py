from datetime import datetime
from typing import Any, List

import clickhouse_connect
from sqlalchemy import Boolean, Column, DateTime, Integer, Numeric, String

from dwh.core.domain.load_passengers_job import ITitanicPassengerRepository
from dwh.core.entities.passenger import Passenger
from dwh.core.repository.entity_base import EntityBase
from dwh.utils.postgres import ChConnect


class PassengerEntity(EntityBase):
    """
    CREATE TABLE IF NOT EXISTS de.passengers(
        age Float64 NULL,
        fare Float64 NOT NULL,
        name VARCHAR(255) NOT NULL,
        p_class INT NOT NULL,
        parents_children_aboard INT NOT NULL,
        gender INT NOT NULL,
        siblings_spouses_aboard INT NOT NULL,
        survived BOOLEAN NOT NULL,
    )
    ENGINE MergeTree
    ORDER BY name;
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


def to_entity(passenger: Passenger) -> List[Any]:
    return [
        passenger.age,
        passenger.fare,
        passenger.name,
        passenger.p_class,
        passenger.parents_children_aboard,
        passenger.gender,
        passenger.siblings_spouses_aboard,
        passenger.survived,
    ]


class TitanicPassengerClickhouseRepository(ITitanicPassengerRepository):
    def __init__(self, ch: ChConnect) -> None:
        self.client = clickhouse_connect.get_client(
            host=ch.host, port=ch.port, username=ch.user, database=ch.db_name, password=ch.pw
        )

    def save(self, passenger: Passenger):
        passengers_ch = [to_entity(passenger)]
        self.client.insert(
            "de.passengers",
            passengers_ch,
            column_names=[
                "age",
                "fare",
                "name",
                "p_class",
                "parents_children_aboard",
                "gender",
                "siblings_spouses_aboard",
                "survived",
            ],
        )

    def save_many(self, passengers: List[Passenger]):
        passengers_ch = [to_entity(p) for p in passengers]
        self.client.insert(
            "de.passengers",
            passengers_ch,
            column_names=[
                "age",
                "fare",
                "name",
                "p_class",
                "parents_children_aboard",
                "gender",
                "siblings_spouses_aboard",
                "survived",
            ],
        )
