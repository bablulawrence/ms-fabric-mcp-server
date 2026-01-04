# ABOUTME: Semantic model column definition and data types.
# ABOUTME: Provides Pydantic models for semantic model schema operations.
"""Semantic model models."""

from enum import Enum

from pydantic import BaseModel, Field


class DataType(str, Enum):
    STRING = "string"
    INT64 = "int64"
    DECIMAL = "decimal"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    DATETIME = "dateTime"


class SemanticModelColumn(BaseModel):
    """Column configuration for semantic models."""

    name: str = Field(description="Name of the column")
    data_type: DataType = Field(description="Data type of the column")


class SemanticModelMeasure(BaseModel):
    """Measure configuration for semantic models."""

    name: str = Field(description="Name of the measure")
    expression: str = Field(description="DAX expression for the measure")
    format_string: str | None = Field(default=None, description="Format string")
    display_folder: str | None = Field(default=None, description="Display folder path")
    description: str | None = Field(default=None, description="Measure description")
