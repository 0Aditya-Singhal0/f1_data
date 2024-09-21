from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, MetaData, Table
import pandas as pd

Base = declarative_base()
metadata = MetaData()


def create_dynamic_model(table_name, df):
    columns = []
    for column_name, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            column_type = Integer
        elif "float" in str(dtype):
            column_type = Float
        elif "datetime" in str(dtype):
            column_type = DateTime
        else:
            column_type = String

        columns.append(Column(column_name, column_type))

    dynamic_table = Table(table_name, metadata, *columns)
    return dynamic_table
