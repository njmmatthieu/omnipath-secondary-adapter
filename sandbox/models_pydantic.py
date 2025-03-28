import pandas as pd
import pandera as pa

from pandera.typing import Series

# data to validate
df = pd.read_csv(
    "/home/ecarreno/SSC-Projects/b_REPOSITORIES/ecarrenolozano/omnipath-secondary-adapter/data_testing/networks/subset_interactions_edgecases.tsv"
)


class NetworksSchema(pa.DataFrameModel):
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
    sources: Series[object] = pa.Field(  # Stores lists (ARRAY equivalent)
        coerce=True,
        checks=pa.Check(
            lambda x: all(
                isinstance(i, list) and all(isinstance(j, str) for j in i) for i in x
            ),
            error="Each entry must be a list of strings",
        ),
    )
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
    dorothea_level: Series[object] = pa.Field(  # Stores lists (ARRAY equivalent)
        coerce=True,
        checks=pa.Check(
            lambda x: all(
                isinstance(i, list) and all(isinstance(j, str) for j in i) for i in x
            ),
            error="Each entry must be a list of strings",
        ),
    )
    type: Series[str]
    curation_effort: Series[int]
    extra_attrs: Series[object] = pa.Field(
        nullable=True,  # Allows NULL (None) values
        coerce=True,  # Ensures correct type
        checks=pa.Check(
            lambda x: all(isinstance(i, (dict, type(None))) for i in x),
            error="Each entry must be a dictionary or None",
        ),
    )
    evidences: Series[object] = pa.Field(
        nullable=True,  # Allows NULL (None) values
        coerce=True,  # Ensures correct type
        checks=pa.Check(
            lambda x: all(isinstance(i, (dict, type(None))) for i in x),
            error="Each entry must be a dictionary or None",
        ),
    )
    ncbi_tax_id_source: Series[int]
    entity_type_source: Series[str]
    ncbi_tax_id_target: Series[int]
    entity_type_target: Series[str]


print(NetworksSchema.validate(df))
