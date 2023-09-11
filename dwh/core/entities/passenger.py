from pydantic import BaseModel

from dwh.core.entities.gender import Gender


class Passenger(BaseModel):
    age: float
    fare: float
    name: str
    p_class: int
    parents_children_aboard: int
    gender: Gender
    siblings_spouses_aboard: int
    survived: bool
