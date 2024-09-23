from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    BigInteger,
    Float,
    DateTime,
    MetaData,
    Table,
    PrimaryKeyConstraint,
    Boolean,
    JSON,
    Text,
)
import pandas as pd

Base = declarative_base()
metadata = MetaData()


def create_dynamic_model(table_name, df, primary_keys=None):
    columns = []
    existing_columns = (
        set(metadata.tables[table_name].columns.keys())
        if table_name in metadata.tables
        else set()
    )

    for column_name, dtype in zip(df.columns, df.dtypes):
        if column_name in existing_columns:
            continue  # Skip columns that already exist

        if pd.api.types.is_integer_dtype(dtype):
            column_type = BigInteger
        elif pd.api.types.is_float_dtype(dtype):
            column_type = Float
        elif pd.api.types.is_bool_dtype(dtype):
            column_type = Boolean
        elif (
            pd.api.types.is_datetime64_any_dtype(dtype) or "date" in column_name.lower()
        ):
            column_type = DateTime
        elif pd.api.types.is_object_dtype(dtype):
            # Handle columns containing lists or dicts
            sample_value = (
                df[column_name].dropna().iloc[0]
                if not df[column_name].dropna().empty
                else None
            )
            if isinstance(sample_value, (list, dict)):
                column_type = JSON
            else:
                column_type = Text  # Use Text for large strings
        else:
            column_type = Text  # Fallback to Text for any other types

        column = Column(column_name, column_type)
        columns.append(column)

    if table_name in metadata.tables:
        # Existing table
        table = metadata.tables[table_name]
        for column in columns:
            if column.name not in table.columns:
                column.create(table, populate_default=False)
    else:
        # Create new table
        if primary_keys:
            pk_constraint = PrimaryKeyConstraint(*primary_keys, name=f"{table_name}_pk")
            table = Table(table_name, metadata, *columns, pk_constraint)
        else:
            table = Table(table_name, metadata, *columns)
    return table
