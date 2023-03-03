from dataclasses import dataclass


@dataclass
class IncorrectDatabase(Exception):
    def __str__(self) -> str:
        return "IncorrectDatabase"
