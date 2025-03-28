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
    pandera_python_mapping = {
        pa.typing.pandas.Series[str]: "string",
        pa.typing.pandas.Series[int]: "int",
        pa.typing.pandas.Series[bool]: "bool",
        pa.typing.pandas.Series[float]: "float",
    }

    return pandera_python_mapping.get(pandera_datatype, DEFAULT_PANDAS_TYPE)


# -----------------------------------------------------------------------
# -----------------     Pandera DataFrame Models      -------------------
# -----------------------------------------------------------------------
class NetworksPanderaModel(pa.DataFrameModel):
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
    is_directed: Series[bool] = pa.Field(nullable=False, coerce=True)
    is_stimulation: Series[bool] = pa.Field(nullable=False)
    is_inhibition: Series[bool] = pa.Field(nullable=False)
    consensus_direction: Series[bool] = pa.Field(nullable=False)
    consensus_stimulation: Series[bool] = pa.Field(nullable=False)
    consensus_inhibition: Series[bool] = pa.Field(nullable=False)
    sources: Series[str] = pa.Field(nullable=False)
    references: Series[str] = pa.Field(nullable=False)
    omnipath: Series[bool] = pa.Field(nullable=False)
    kinaseextra: Series[bool] = pa.Field(nullable=False)
    ligrecextra: Series[bool] = pa.Field(nullable=False)
    pathwayextra: Series[bool] = pa.Field(nullable=False)
    mirnatarget: Series[bool] = pa.Field(nullable=False)
    dorothea: Series[bool] = pa.Field(nullable=False)
    collectri: Series[bool] = pa.Field(nullable=False)
    tf_target: Series[bool] = pa.Field(nullable=False)
    lncrna_mrna: Series[bool] = pa.Field(nullable=False)
    tf_mirna: Series[bool] = pa.Field(nullable=False)
    small_molecule: Series[bool] = pa.Field(nullable=False)
    dorothea_curated: Series[bool] = pa.Field(nullable=False)
    dorothea_chipseq: Series[bool] = pa.Field(nullable=False)
    dorothea_tfbs: Series[bool] = pa.Field(nullable=False)
    dorothea_coexp: Series[bool] = pa.Field(nullable=False)
    dorothea_level: Series[str] = pa.Field(nullable=False)
    type: Series[str] = pa.Field(nullable=False)
    curation_effort: Series[int] = pa.Field(nullable=False)
    extra_attrs: Series[str] = pa.Field(nullable=True)
    evidences: Series[str] = pa.Field(nullable=True)
    ncbi_tax_id_source: Series[int] = pa.Field(nullable=False)
    entity_type_source: Series[str] = pa.Field(nullable=False)
    ncbi_tax_id_target: Series[int] = pa.Field(nullable=False)
    entity_type_target: Series[str] = pa.Field(nullable=False)

    # ---- Custom methods for the class
    @classmethod
    def _return_pandas_dtypes(cls):
        """Returns a dictionary mapping Pandera types to Pandas dtypes."""
        dtypes = {
            col: _map_pandera_to_pandas_type(dtype)
            for col, dtype in cls.to_schema().columns.items()
        }
        if None in dtypes.values():
            raise ValueError("Unsupported Pandera data type found.")
        return dtypes

    # ---- DataFrame Model Configuration
    class Config:
        name = BASE_SCHEMA_NAME
        strict = True
        coerce = True  # Redundancy here is intended, to force the type conversion.


class AnnotationsPanderaModel(pa.DataFrameModel):
    """Pandera DataFrame Model for Omnipath Annotations Table.
    This schema defines the expected structure of the DataFrame
    containing annotations data, ensuring type and constraint validation.
    """

    __slots__ = ()  # to avoid any possible dynamic creation of attributes (fields)

    # ---- Column: Pandera datatype validator
    source: Series[str] = pa.Field(nullable=True)
    target: Series[str] = pa.Field(nullable=True)
    source_genesymbol: Series[str] = pa.Field(nullable=False)
    target_genesymbol: Series[str] = pa.Field(nullable=False)
    is_directed: Series[bool] = pa.Field(nullable=False, coerce=True)
    is_stimulation: Series[bool] = pa.Field(nullable=False)
    is_inhibition: Series[bool] = pa.Field(nullable=False)
    consensus_direction: Series[bool] = pa.Field(nullable=False)
    consensus_stimulation: Series[bool] = pa.Field(nullable=False)
    consensus_inhibition: Series[bool] = pa.Field(nullable=False)
    sources: Series[str] = pa.Field(nullable=False)
    references: Series[str] = pa.Field(nullable=False)
    omnipath: Series[bool] = pa.Field(nullable=False)
    kinaseextra: Series[bool] = pa.Field(nullable=False)
    ligrecextra: Series[bool] = pa.Field(nullable=False)
    pathwayextra: Series[bool] = pa.Field(nullable=False)
    mirnatarget: Series[bool] = pa.Field(nullable=False)
    dorothea: Series[bool] = pa.Field(nullable=False)
    collectri: Series[bool] = pa.Field(nullable=False)
    tf_target: Series[bool] = pa.Field(nullable=False)
    lncrna_mrna: Series[bool] = pa.Field(nullable=False)
    tf_mirna: Series[bool] = pa.Field(nullable=False)
    small_molecule: Series[bool] = pa.Field(nullable=False)
    dorothea_curated: Series[bool] = pa.Field(nullable=False)
    dorothea_chipseq: Series[bool] = pa.Field(nullable=False)
    dorothea_tfbs: Series[bool] = pa.Field(nullable=False)
    dorothea_coexp: Series[bool] = pa.Field(nullable=False)
    dorothea_level: Series[str] = pa.Field(nullable=False)
    type: Series[str] = pa.Field(nullable=False)
    curation_effort: Series[int] = pa.Field(nullable=False)
    extra_attrs: Series[str] = pa.Field(nullable=True)
    evidences: Series[str] = pa.Field(nullable=True)
    ncbi_tax_id_source: Series[int] = pa.Field(nullable=False)
    entity_type_source: Series[str] = pa.Field(nullable=False)
    ncbi_tax_id_target: Series[int] = pa.Field(nullable=False)
    entity_type_target: Series[str] = pa.Field(nullable=False)

    # ---- Custom methods for the class
    @classmethod
    def _return_pandas_dtypes(cls):
        """Returns a dictionary mapping Pandera types to Pandas dtypes."""
        dtypes = {
            col: _map_pandera_to_pandas_type(dtype)
            for col, dtype in cls.to_schema().columns.items()
        }
        if None in dtypes.values():
            raise ValueError("Unsupported Pandera data type found.")
        return dtypes

    # ---- DataFrame Model Configuration
    class Config:
        name = BASE_SCHEMA_NAME
        strict = True
        coerce = True  # Redundancy here is intended, to force the type conversion.
