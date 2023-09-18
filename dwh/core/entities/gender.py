from enum import IntEnum, unique


@unique
class Gender(IntEnum):
    UNKNOWN = 0
    PREFER_NOT_TO_SAY = 1
    MALE = 2
    FEMALE = 3
