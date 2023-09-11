from pydantic import BaseModel


class Passenger(BaseModel):
    survived: bool
    p_class: int
    name: str
    sex: str
    age: float
    siblings_spouses_aboard: int
    parents_children_aboard: int
    fare: float
