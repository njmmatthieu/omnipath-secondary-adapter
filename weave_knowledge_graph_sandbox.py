#!/usr/bin/env python3

import argparse
import logging
import os
import yaml

import numpy as np
import ontoweaver
import pandas as pd
import pandera as pa
from biocypher import BioCypher

from omnipath_secondary_adapter.models import NetworksPanderaModel

# --------------------------  DATA TYPES FOR PANDAS DATAFRAMES -------------------
MAPPING_DATATYPES_NETWORKS = {
    "source": "string",
    "target": "string",
    "source_genesymbol": "string",
    "target_genesymbol": "string",
    "is_directed": "bool",
    "is_stimulation": "bool",
    "is_inhibition": "bool",
    "consensus_direction": "bool",
    "consensus_stimulation": "bool",
    "consensus_inhibition": "bool",
    "sources": "string",
    "references": "string",
    "omnipath": "bool",
    "kinaseextra": "bool",
    "ligrecextra": "bool",
    "pathwayextra": "bool",
    "mirnatarget": "bool",
    "dorothea": "bool",
    "collectri": "bool",
    "tf_target": "bool",
    "lncrna_mrna": "bool",
    "tf_mirna": "bool",
    "small_molecule": "bool",
    "dorothea_curated": "bool",
    "dorothea_chipseq": "bool",
    "dorothea_tfbs": "bool",
    "dorothea_coexp": "bool",
    "dorothea_level": "string",
    "type": "string",
    "curation_effort": "int",
    "extra_attrs": "string",
    "evidences": "string",
    "ncbi_tax_id_source": "int",
    "entity_type_source": "string",
    "ncbi_tax_id_target": "int",
    "entity_type_target": "string",
}


# ---------------------------------------------------------------------------------
def convert_to_boolean(value):
    if isinstance(value, bool):
        return value
    elif value in [1, "1", "True", True]:
        return True
    elif value in [0, "0", "False", False, "", "nan", np.nan]:
        return False
    else:
        return False


# ---- Steps pipeline


def read_table(path_table):
    # Create table with automatic inference of datatypes
    df = pd.read_table(path_table, sep="\t", keep_default_na=False)

    # ---- Optimize data types
    df = df.astype(MAPPING_DATATYPES_NETWORKS)

    return df


def validate_input_schema(dataframe, dataframe_model):
    try:
        dataframe_model.validate(dataframe)
        print("DataFrame is valid!")
    except pa.errors.SchemaError as e:
        print("DataFrame validation failed:", e)


# from omnipath_seconday_adapter.adapters.example_adapter import

if __name__ == "__main__":

    usage = f"Extract nodes and edges from CSV tables of Omnipath databse: networks, enzyme-PTM, complexes, annotations and intercell."
    parser = argparse.ArgumentParser(description=usage)

    parser.add_argument(
        "-net",
        "--networks",
        metavar="TSV",
        nargs="+",
        help="Extract from the Omnipath networks TSV file.",
    )

    parser.add_argument(
        "-enz",
        "--enzyme-PTM",
        metavar="TSV",
        nargs="+",
        help="Extract from the Omnipath enzyme-PTM TSV file.",
    )

    parser.add_argument(
        "-co",
        "--complexes",
        metavar="TSV",
        nargs="+",
        help="Extract from the Omnipath complexes TSV file.",
    )

    parser.add_argument(
        "-an",
        "--annotations",
        metavar="TSV",
        nargs="+",
        help="Extract from the Omnipath annotations TSV file.",
    )

    parser.add_argument(
        "-int",
        "--intercell",
        metavar="TSV",
        nargs="+",
        help="Extract from the Omnipath intercell TSV file.",
    )

    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    parser.add_argument(
        "-v",
        "--verbose",
        choices=levels.keys(),
        default="WARNING",
        help="Set the verbose level (default: %(default)s).",
    )

    asked = parser.parse_args()

    # Actually extract data.
    nodes = []
    edges = []

    # Extract from databases not requiring preprocessing.
    if asked.networks:
        logging.info(f"Weave Omnipath networks data...")

        # ---- Step 0. Download resources

        # ---- Step 1. Read the networks table (input)
        networks_df = read_table(path_table=asked.networks[0])

        # ---- Step 2. Validate if networks table complies schema
        validate_input_schema(dataframe=networks_df, dataframe_model=NetworksPanderaModel)

        # ---- Step 3. Extract nodes and edges using ontoweaver
        mapping_file = "./omnipath_secondary_adapter/adapters/networks.yaml"
        with open(mapping_file) as fd:
            mapping = yaml.full_load(fd)

        adapter = ontoweaver.tabular.extract_table(
            # df=networks_df, config=mapping, separator=":", affix="suffix"
            df=networks_df,
            config=mapping,
            separator=":",
            affix="none",
            parallel_mapping=min(32, (os.cpu_count() or 1) + 4),
        )

        nodes += adapter.nodes
        edges += adapter.edges

        logging.info(f"Wove Networks: {len(nodes)} nodes, {len(edges)} edges.")

    # ---- Step 4. Fusion and writing
    import_file = ontoweaver.reconciliate_write(
        nodes,
        edges,
        "config/biocypher_config.yaml",
        "config/schema_config.yaml",
        separator=", ",
    )

    # TODO ---- Step 5. Validate outputs vs. schema_config.yaml

    # TODO ---- Step 6. Validate outputs vs. schema_config.yaml

# Example of usage in CLI:
# poetry run python weave_knowledge_graph.py -net ./data_testing/subset_interactions.tsv
# poetry run python -m cProfile -s time weave_knowledge_graph.py -net ./data/subset_interactions_100.tsv
