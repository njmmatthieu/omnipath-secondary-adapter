from functools import lru_cache

import pandas as pd
import pandera as pa
from pandera.typing import Series


BASE_SCHEMA_NAME = "BaseSchema"
DEFAULT_PANDAS_TYPE = "object"


def _map_pandera_to_pandas_type(pandera_datatype: pa.typing.pandas.Series) -> str:
    """
    Map Pandera column types to pandas data types.

    Args:
        pandera_datatype: The Pandera data type to map.

    Returns:
        str: The corresponding pandas data type as a string.
    """
    mapping = {
        pa.dtypes.String: "string",
        pa.dtypes.Int: "Int64",
        pa.dtypes.Float: "Float64",
        pa.dtypes.Bool: "boolean",
        pa.dtypes.DateTime: "datetime64[ns]",
        pa.dtypes.Category: "category",
    }

    # Find the base type from the type hierarchy
    for base_type, pandas_type in mapping.items():
        if isinstance(pandera_datatype, base_type):
            return pandas_type

    return DEFAULT_PANDAS_TYPE  # fallback


# -----------------------------------------------------------------------
# -----------------     Pandera DataFrame Models      -------------------
# -----------------------------------------------------------------------
# Create a base class with the common method
class BasePanderaModel(pa.DataFrameModel):
    """Base class for Pandera DataFrame Models with common functionality."""

    @classmethod
    @lru_cache(maxsize=None)
    def _return_pandas_dtypes(cls):
        """Returns a dictionary mapping Pandera types to Pandas dtypes."""
        dtypes = {
            col: _map_pandera_to_pandas_type(value.dtype)
            for col, value in cls.to_schema().columns.items()
        }
        if None in dtypes.values():
            raise ValueError("Unsupported Pandera data type found.")
        return dtypes

    class Config:
        strict = True
        coerce = False  # Redundancy here is intended, to force the type conversion.


class NetworksPanderaModel(BasePanderaModel):
    """Pandera DataFrame Model for Omnipath Interactions Table.
    This schema defines the expected structure of the DataFrame
    containing interaction data, ensuring type and constraint validation.
    """

    __slots__ = ()  # to avoid any possible dynamic creation of attributes (fields)

    # ---- Column: Pandera datatype validator
    source: Series[str] = pa.Field(nullable=True)
    target: Series[str] = pa.Field(nullable=True)
    source_genesymbol: Series[str] = pa.Field(nullable=False)
    target_genesymbol: Series[str] = pa.Field(nullable=False)
    is_directed: Series[bool] = pa.Field(nullable=False)
    is_stimulation: Series[bool] = pa.Field(nullable=False)
    is_inhibition: Series[bool] = pa.Field(nullable=False)
    consensus_direction: Series[bool] = pa.Field(nullable=False)
    consensus_stimulation: Series[bool] = pa.Field(nullable=False)
    consensus_inhibition: Series[bool] = pa.Field(nullable=False)
    sources: Series[str] = pa.Field(nullable=False)
    references: Series[str] = pa.Field(nullable=True)
    omnipath: Series[bool] = pa.Field(nullable=False)
    kinaseextra: Series[bool] = pa.Field(nullable=False)
    ligrecextra: Series[bool] = pa.Field(nullable=False)
    pathwayextra: Series[bool] = pa.Field(nullable=False)
    mirnatarget: Series[bool] = pa.Field(nullable=False)
    dorothea: Series[bool] = pa.Field(nullable=False)
    collectri: Series[bool] = pa.Field(nullable=False)
    tf_target: Series[bool] = pa.Field(nullable=True)
    lncrna_mrna: Series[bool] = pa.Field(nullable=False)
    tf_mirna: Series[bool] = pa.Field(nullable=False)
    small_molecule: Series[bool] = pa.Field(nullable=False)
    dorothea_curated: Series[bool] = pa.Field(nullable=True)
    dorothea_chipseq: Series[bool] = pa.Field(nullable=True)
    dorothea_tfbs: Series[bool] = pa.Field(nullable=True)
    dorothea_coexp: Series[bool] = pa.Field(nullable=True)
    dorothea_level: Series[str] = pa.Field(nullable=True)
    type: Series[str] = pa.Field(nullable=False)
    curation_effort: Series[int] = pa.Field(nullable=False)
    extra_attrs: Series[str] = pa.Field(nullable=True)
    evidences: Series[str] = pa.Field(nullable=True)
    ncbi_tax_id_source: Series[int] = pa.Field(nullable=False)
    entity_type_source: Series[str] = pa.Field(nullable=False)
    ncbi_tax_id_target: Series[int] = pa.Field(nullable=False)
    entity_type_target: Series[str] = pa.Field(nullable=False)

    # ---- DataFrame Model Configuration
    class Config(BasePanderaModel.Config):
        name = BASE_SCHEMA_NAME


class EnzymePTMPanderaModel(BasePanderaModel):
    """Pandera DataFrame Model for Omnipath Interactions Table.
    This schema defines the expected structure of the DataFrame
    containing interaction data, ensuring type and constraint validation.
    """

    __slots__ = ()  # to avoid any possible dynamic creation of attributes (fields)

    # ---- Column: Pandera datatype validator
    enzyme: Series[str] = pa.Field(nullable=False)
    enzyme_genesymbol: Series[str] = pa.Field(nullable=False)
    substrate: Series[str] = pa.Field(nullable=False)
    substrate_genesymbol: Series[str] = pa.Field(nullable=False)
    isoforms: Series[str] = pa.Field(nullable=False)
    residue_type: Series[str] = pa.Field(nullable=False)
    residue_offset: Series[int] = pa.Field(nullable=False)
    modification: Series[str] = pa.Field(nullable=False)
    sources: Series[str] = pa.Field(nullable=False)
    references: Series[str] = pa.Field(nullable=True)
    curation_effort: Series[int] = pa.Field(nullable=False)
    ncbi_tax_id: Series[int] = pa.Field(nullable=False)

    # ---- DataFrame Model Configuration
    class Config(BasePanderaModel.Config):
        name = BASE_SCHEMA_NAME
