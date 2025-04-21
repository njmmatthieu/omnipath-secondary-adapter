import pandas as pd
import pandera as pa

from pandera.typing import Series


# data to validate
df = pd.read_table("data_testing/networks/subset_interactions_edgecases.tsv")


class NetworksSchema(pa.DataFrameModel):
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
    small_molecule: Series[bool] = pa.Field(nullable=True)
    dorothea_curated: Series[str] = pa.Field(nullable=False)
    dorothea_chipseq: Series[str] = pa.Field(nullable=False)
    dorothea_tfbs: Series[str] = pa.Field(nullable=False)
    dorothea_coexp: Series[str] = pa.Field(nullable=False)
    dorothea_level: Series[str] = pa.Field(nullable=False)
    type: Series[str] = pa.Field(nullable=False)
    curation_effort: Series[int] = pa.Field(nullable=False)
    extra_attrs: Series[str] = pa.Field(nullable=True)
    evidences: Series[str] = pa.Field(nullable=True)
    ncbi_tax_id_source: Series[int] = pa.Field(nullable=False)
    entity_type_source: Series[str] = pa.Field(nullable=False)
    ncbi_tax_id_target: Series[int] = pa.Field(nullable=False)
    entity_type_target: Series[str] = pa.Field(nullable=False)
