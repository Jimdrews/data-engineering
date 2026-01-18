"""Base class for Pydantic models with Spark DataFrame conversion."""

from __future__ import annotations

from typing import TypeVar

from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

T = TypeVar("T", bound="SparkModel")

PYTHON_TO_SPARK_TYPES = {
    str: StringType(),
    int: LongType(),
    float: DoubleType(),
    bool: BooleanType(),
}


class SparkModel(BaseModel):
    """Pydantic BaseModel with built-in Spark DataFrame conversion methods."""

    @classmethod
    def spark_schema(cls) -> StructType:
        """Convert this model class to a PySpark StructType schema.

        Returns:
            A PySpark StructType representing the model's schema.

        Raises:
            TypeError: If a field type is not supported.
        """
        fields = []
        for field_name, field_info in cls.model_fields.items():
            annotation = field_info.annotation

            # Handle Optional types
            origin = getattr(annotation, "__origin__", None)
            if origin is type(None):
                continue
            if origin is not None:
                args = getattr(annotation, "__args__", ())
                non_none_args = [a for a in args if a is not type(None)]
                if non_none_args:
                    annotation = non_none_args[0]

            spark_type = PYTHON_TO_SPARK_TYPES.get(annotation)
            if spark_type is None:
                raise TypeError(
                    f"Unsupported type {annotation} for field {field_name}. "
                    f"Supported types: {list(PYTHON_TO_SPARK_TYPES.keys())}"
                )

            nullable = not field_info.is_required()
            fields.append(StructField(field_name, spark_type, nullable))

        return StructType(fields)

    @classmethod
    def to_dataframe(
        cls: type[T],
        spark: SparkSession,
        models: list[T],
    ) -> DataFrame:
        """Convert a list of model instances to a PySpark DataFrame.

        Args:
            spark: Active SparkSession.
            models: List of model instances.

        Returns:
            A PySpark DataFrame containing the model data.
        """
        schema = cls.spark_schema()
        rows = [tuple(m.model_dump().values()) for m in models]
        return spark.createDataFrame(rows, schema)

    @classmethod
    def from_dataframe(cls: type[T], df: DataFrame) -> list[T]:
        """Convert a PySpark DataFrame to a list of model instances.

        Args:
            df: A PySpark DataFrame.

        Returns:
            A list of validated model instances.

        Raises:
            ValidationError: If any row fails Pydantic validation.
        """
        rows = df.collect()
        return [cls(**row.asDict()) for row in rows]
