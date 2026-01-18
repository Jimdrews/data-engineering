"""Employee model matching existing notebook data."""

from typing import Annotated

from pydantic import Field

from models.base import SparkModel


class Employee(SparkModel):
    """Employee data model with validation."""

    name: Annotated[str, Field(min_length=1, description="Employee name")]
    department: Annotated[str, Field(min_length=1, description="Department name")]
    salary: Annotated[int, Field(gt=0, description="Annual salary in dollars")]
