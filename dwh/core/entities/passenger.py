from pydantic import BaseModel


class Passenger(BaseModel):
    name: str
    age: float
    survived: bool
    p_class: int
    sex: str
    siblings_spouses_aboard: int
    parents_children_aboard: int
    fare: float
