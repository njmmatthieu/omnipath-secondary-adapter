from datetime import datetime
from itertools import zip_longest

from pydantic import BaseModel

from models_pandera import NetworksSchema


VALID_EXTENSIONS = (
    ".csv",
    ".tar.gz",
    ".tsv",
)


import datetime

import pandera as pa
import pandas as pd
from pandera import (
    Column,
    DataFrameSchema,
    DataFrameModel,
)
from pandera.typing import Index, DataFrame, Series
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import declarative_base

# from sqlalchemy import (
#     ARRAY,
#     Column,
#     String,
#     Boolean,
#     Integer,
# )
# from sqlalchemy.sql.sqltypes import (
#     String,
#     Integer,
#     Float,
#     Boolean,
#     Date,
#     DateTime,
#     ARRAY,
# )

from pandera.typing.common import (
    Bool,
    Float,
    Int,
    String,
)

Base = declarative_base()


def map_pandera_to_pandas_type(pandera_datatype):
    """
    Map Pandera column types to pandas data types.
    """
    pandera_python_mapping = {
        pa.typing.pandas.Series[str]: "string",
        pa.typing.pandas.Series[int]: "int",
        pa.typing.pandas.Series[bool]: "bool",
        pa.typing.pandas.Series[float]: "float",
    }

    if pandera_datatype in pandera_python_mapping:
        return pandera_python_mapping.get(pandera_datatype, "object")


# def sqlalchemy_column_to_pandera(sqlalchemy_column):
#     """Convert SQLAlchemy column type to Pandera column."""

#     # Mapping SQLAlchemy types to Pandera
#     type_mapping = {
#         String: str,
#         Integer: int,
#         Boolean: bool,
#         ARRAY: object,
#         JSONB: object,
#     }

#     col_type = sqlalchemy_column.type  # Get the SQLAlchemy type
#     nullable = sqlalchemy_column.nullable  # Check if it's nullable

#     # Determine the corresponding Pandera type
#     pandera_type = type_mapping.get(
#         type(col_type), object
#     )  # Default to object if unknown

#     return pa.Column(
#         dtype=pandera_type,
#         nullable=nullable,
#     )


# ------------------------  ORIGINAL SCHEMA FOR INTERACTIONS DATABASE
# class Interactions(Base):
#     """
#     Definition for the `interactions` table columns and types.
#     """

#     __tablename__ = "interactions"
#     id = Column(Integer, primary_key=True)
#     source = Column(String, nullable=True)
#     target = Column(String, nullable=True)
#     source_genesymbol = Column(String)
#     target_genesymbol = Column(String)
#     is_directed = Column(Boolean)
#     is_stimulation = Column(Boolean)
#     is_inhibition = Column(Boolean)
#     consensus_direction = Column(Boolean)
#     consensus_stimulation = Column(Boolean)
#     consensus_inhibition = Column(Boolean)
#     sources = Column(ARRAY(String))
#     references = Column(String)
#     omnipath = Column(Boolean)
#     kinaseextra = Column(Boolean)
#     ligrecextra = Column(Boolean)
#     pathwayextra = Column(Boolean)
#     mirnatarget = Column(Boolean)
#     dorothea = Column(Boolean)
#     collectri = Column(Boolean)
#     tf_target = Column(Boolean)
#     lncrna_mrna = Column(Boolean)
#     tf_mirna = Column(Boolean)
#     small_molecule = Column(Boolean)
#     dorothea_curated = Column(Boolean)
#     dorothea_chipseq = Column(Boolean)
#     dorothea_tfbs = Column(Boolean)
#     dorothea_coexp = Column(Boolean)
#     dorothea_level = Column(ARRAY(String))
#     type = Column(String)
#     curation_effort = Column(Integer)
#     extra_attrs = Column(JSONB, nullable=True)
#     evidences = Column(JSONB, nullable=True)
#     ncbi_tax_id_source = Column(Integer)
#     entity_type_source = Column(String)
#     ncbi_tax_id_target = Column(Integer)
#     entity_type_target = Column(String)

#     @classmethod
#     def return_column_dtypes(cls):
#         columns = inspect(cls).c
#         dictionary = {}

#         for column in columns:
#             if str(column.name).lower() == "id":
#                 continue
#             dictionary[column.name] = map_sqlalchemy_to_python_type(column.type)

#         return dictionary

#     @classmethod
#     def return_pandera_dataframe_schema(cls):
#         columns = inspect(cls).c
#         dictionary = {}

#         for column in columns:
#             if str(column.name).lower() == "id":  # Skips the index column
#                 continue
#             dictionary[column.name] = sqlalchemy_column_to_pandera(column)

#         pandera_dataframe_schema = pa.DataFrameSchema(
#             columns=dictionary, coerce=True, strict=True
#         )

#         return pandera_dataframe_schema


class Interactions(pa.DataFrameModel):
    source: Series[str] = pa.Field(nullable=True)
    target: Series[str] = pa.Field(nullable=True)
    source_genesymbol: Series[str]
    target_genesymbol: Series[str]
    is_directed: Series[bool]
    is_stimulation: Series[bool]
    is_inhibition: Series[bool]
    consensus_direction: Series[bool]
    consensus_stimulation: Series[bool]
    consensus_inhibition: Series[bool]
    sources: Series[str]
    references: Series[str]
    omnipath: Series[bool]
    kinaseextra: Series[bool]
    ligrecextra: Series[bool]
    pathwayextra: Series[bool]
    mirnatarget: Series[bool]
    dorothea: Series[bool]
    collectri: Series[bool]
    tf_target: Series[bool]
    lncrna_mrna: Series[bool]
    tf_mirna: Series[bool]
    small_molecule: Series[bool]
    dorothea_curated: Series[str]
    dorothea_chipseq: Series[str]
    dorothea_tfbs: Series[str]
    dorothea_coexp: Series[str]
    dorothea_level: Series[str]
    type: Series[str]
    curation_effort: Series[int]
    extra_attrs: Series[str] = pa.Field(nullable=True)
    evidences: Series[str] = pa.Field(nullable=True)
    ncbi_tax_id_source: Series[int]
    entity_type_source: Series[str]
    ncbi_tax_id_target: Series[int]
    entity_type_target: Series[str]

    class Config:
        name = "BaseSchema"
        strict = True
        coerce = True

    @classmethod
    def return_pandas_dtypes(cls):
        columns = cls.__annotations__

        mapping_datatypes = {}

        for column, datatype in columns.items():
            if str(column).lower() == "id":
                continue
            mapping_datatypes[column] = map_pandera_to_pandas_type(datatype)

        return mapping_datatypes


for k, v in Interactions.return_pandas_dtypes().items():
    print(f"{k}:{v}")


# --------------    CUSTOM CLASSES
class InvalidFileExtensionError(Exception):
    """Custom exception for invalid file extension."""

    pass


class Delivery(BaseModel):
    timestamp: datetime
    dimensions: tuple[int, int]


# --------------    CUSTOM FUNCTIONS
def verify_encoding(file_path, encoding="utf-8"):
    try:
        with open(file_path, "r", encoding=encoding, errors="strict") as f:
            # Read the file to check if it matches the encoding
            f.read()
        print(f"[PASSED] File is valid with {encoding} encoding.")
    except UnicodeDecodeError as e:
        print(f"File is not valid with {encoding} encoding. Error: {e}")
    except Exception as e:
        print(f"Error: {e}")


def verify_extension(file_path: str) -> bool:
    if not file_path.lower().endswith(VALID_EXTENSIONS):
        raise InvalidFileExtensionError(
            f"Invalid file extension: {file_path}. Expected one of {VALID_EXTENSIONS}."
        )
    else:
        print(f"[PASSED] Valid Extension!")


def verify_header(file_path, encoding="utf-8"):

    # Read header from current file
    with open(file_path, "r", encoding=encoding) as file:
        first_line = tuple(file.readline().strip().split())

    # Extract columns from schema (order is guaranted in Python > 3.6)
    columns_from_schema = tuple(
        NetworksSchema.get_metadata()["NetworksSchema"]["columns"].keys()
    )

    for column_header, column_schema in zip_longest(
        first_line, columns_from_schema, fillvalue=None
    ):
        if column_header != column_schema:
            print("[NOT PASSED] Verify Header")
            print(
                f"\tColumn in header: '{column_header}' is different from '{column_schema}' in schema"
            )


def main():

    file_path = "/home/ecarreno/SSC-Projects/b_REPOSITORIES/ecarrenolozano/omnipath-secondary-adapter/data/subset_interactions_complete.tsv"

    verify_extension(file_path=file_path)

    verify_encoding(file_path=file_path)

    verify_header(file_path)


if __name__ == "__main__":

    main()
