#!/usr/bin/env python3
"""
Script Name: Omnipath Adapter (secondary type)
Description:
    This script retrieves information from Omnipath databases

Usage:
    Example 1:
    # Download and process the latest Omnipath file (dataset) from Omnipath Archive.
    poetry run python weave_knowledge_graph_backup.py \
    -net download

    Example 2:
    # Process the file pointed i.e., ./data_testing/networks/subset_interactions_edgecases.tsv
    poetry run python weave_knowledge_graph.py \
    -net ./data_testing/networks/subset_interactions_edgecases.tsv

Arguments:
    -net, --networks        Path to the 'networks' dataset, or download latest from archive.
    -enz, --enzyme-PTM      Path to the 'enz-PTM' dataset, or download latest from archive.
    -co, --complexes        Path to the 'complexes' dataset, or download latest from archive.
    -an, --annotations      Path to the 'annotations' dataset, or download latest from archive.
    -inter, --intercell     Path to the 'intercell' dataset, or download latest from archive.
    -v, --verbose

"""


import argparse
import logging
import os
import yaml
import sys
from typing import Dict, Any

import ontoweaver
import pandas as pd
from biocypher._get import (
    Downloader,
    FileDownload,
)


# ----------------------    CONSTANTS    ----------------------
CACHE_DIRECTORY = "./data"

URLS_OMNIPATH = {
    "annotations": "https://archive.omnipathdb.org/omnipath_webservice_annotations__latest.tsv.gz",
    "complexes": "https://archive.omnipathdb.org/omnipath_webservice_complexes__latest.tsv.gz",
    "enz_PTM": "https://archive.omnipathdb.org/omnipath_webservice_enz_sub__latest.tsv.gz",
    "intercell": "https://archive.omnipathdb.org/omnipath_webservice_intercell__latest.tsv.gz",
    "networks": "https://archive.omnipathdb.org/omnipath_webservice_interactions__latest.tsv.gz",
}

ONTOWEAVER_MAPPING_FILES = {
    "networks": "./omnipath_secondary_adapter/adapters/networks.yaml"
}

BIOCYPHER_CONFIG_PATH = "config/biocypher_config.yaml"
SCHEMA_PATH = "config/schema_config.yaml"


# ----------------------    HELPER FUNCTIONS    ----------------------
def parse_arguments():
    """
    Extract nodes and edges from CSV tables of Omnipath database: networks, enzyme-PTM, complexes, annotations and intercell.

    This function uses 'argparse' to define and parse arguments related to different
    Omnipath data sources, including networks, enzyme-PTM interactions, complexes,
    annotations, and intercellular interactions. Additionally, it allows setting the
    verbosity level for logging.

    Arguments:
        -net, --networks        Path to the 'networks' dataset, or download latest from archive.
        -enz, --enzyme-PTM      Path to the 'enz-PTM' dataset, or download latest from archive.
        -co, --complexes        Path to the 'complexes' dataset, or download latest from archive.
        -an, --annotations      Path to the 'annotations' dataset, or download latest from archive.
        -inter, --intercell     Path to the 'intercell' dataset, or download latest from archive.
        -v, --verbose

    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.

    """

    usage = "Extract nodes and edges from CSV tables of Omnipath database: networks, enzyme-PTM, complexes, annotations and intercell."

    epilog = """Example Usage:
        poetry run python weave_knowledge_graph_backup.py -net download
        poetry run python weave_knowledge_graph.py -net ./data_testing/networks/subset_interactions_edgecases.tsv
    """

    parser = argparse.ArgumentParser(
        description=usage, epilog=epilog, formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-net",
        "--networks",
        metavar="TSV",
        nargs="?",
        help="extract from the Omnipath 'networks' TSV file.",
    )

    parser.add_argument(
        "-enz",
        "--enzyme-PTM",
        metavar="TSV",
        nargs="?",
        help="extract from the Omnipath 'enzyme-PTM' TSV file.",
    )

    parser.add_argument(
        "-co",
        "--complexes",
        metavar="TSV",
        nargs="?",
        help="extract from the Omnipath 'complexes' TSV file.",
    )

    parser.add_argument(
        "-an",
        "--annotations",
        metavar="TSV",
        nargs="?",
        help="extract from the Omnipath 'annotations' TSV file.",
    )

    parser.add_argument(
        "-inter",
        "--intercell",
        metavar="TSV",
        nargs="?",
        help="extract from the Omnipath 'intercell' TSV file.",
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
        help="set the verbose level (default: %(default)s).",
    )

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)  # Exit with error code 1

    return parser.parse_args()


def setup_logging(level: int | str) -> None:
    """Configure logging."""
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")


def download_resource(resource_name: str, url_resource: str) -> list:

    # Define the directory where the data will be store
    cache_directory = CACHE_DIRECTORY

    # Instantiate the Downloader class
    downloader = Downloader(cache_dir=cache_directory)

    # Create a resource to download
    file_resource = FileDownload(
        name="omnipath_" + resource_name,
        url_s=url_resource,
        lifetime=7,  # Cache for 7 days
    )

    # Download the resource and store the paths where it is stored
    paths = downloader.download(file_resource)

    return paths


def access_to_resource(resource_name: str, argument_resource: str) -> str:

    logging.info(f"Processing resource: {resource_name} with option: {argument_resource}")

    if argument_resource is None:
        logging.error("Invalid option: None. Use --help for guidance.")
        raise ValueError("Invalid option: None. Consult the menu using --help.")

    if argument_resource == "download":
        url = URLS_OMNIPATH.get(resource_name)
        if not url:
            logging.error(f"No download URL found for resource: {resource_name}")
            raise ValueError(f"Download URL not found for resource: {resource_name}")

        paths = download_resource(resource_name=resource_name, url_resource=url)
        if not paths:
            logging.error(f"Download failed for resource: {resource_name}")
            raise RuntimeError(f"Failed to download resource: {resource_name}")

        return paths[0]

    if os.path.isfile(argument_resource):
        return argument_resource

    logging.error(f"Invalid option provided: {argument_resource}")
    raise ValueError(f"Invalid option: {argument_resource}")


def load_data(resource_path: str) -> pd.DataFrame:
    dataframe_resource = pd.read_csv(resource_path, sep="\t")

    return dataframe_resource


def filtering_data(resource_name: str, dataframe: pd.DataFrame) -> pd.DataFrame:
    if resource_name == "annotations":
        dataframe = dataframe

    if resource_name == "complexes":
        dataframe = dataframe

    if resource_name == "enzyme_PTM":
        dataframe = dataframe

    if resource_name == "intercell":
        dataframe = dataframe

    if resource_name == "networks":
        dataframe = dataframe[dataframe.omnipath == True]

    return dataframe


def extract_nodes_edges_ontoweaver(resource_name: str, dataframe_resource: pd.DataFrame):

    # Read Ontoweaver mapping file
    mapping_file = ONTOWEAVER_MAPPING_FILES.get(resource_name)
    if mapping_file is None:
        raise ValueError(f"No mapping file found for resource: {resource_name}")

    try:
        with open(mapping_file) as fd:
            mapping = yaml.full_load(fd)
    except FileNotFoundError:
        raise FileNotFoundError(f"Mapping file not found: {mapping_file}")
    except Exception as e:
        raise RuntimeError(f"An error occurred while reading the mapping file: {e}")

    # Extract nodes and edges with Ontoweaver
    print("Ontoweaver adapter start")
    adapter = ontoweaver.tabular.extract_table(
        df=dataframe_resource,
        config=mapping,
        separator=":",
        affix="none",
    )
    print("Ontoweaver adapter end")

    return adapter


def fuse_and_write(nodes, edges):
    """Fuse duplicated nodes and edges and write the output."""
    print("Fuse start")
    return ontoweaver.reconciliate_write(
        nodes=nodes,
        edges=edges,
        biocypher_config_path=BIOCYPHER_CONFIG_PATH,
        schema_path=SCHEMA_PATH,
        separator=", ",
    )
    print("Fuse end")


def process_resource(resource_name: str, argument_resource: str):
    """Process a given resource, extract nodes and edges, and update the lists."""

    print(f"Resource Option: {argument_resource}")
    print(f"Resource Name: {resource_name}")

    # EXTRACTION
    path_resource = access_to_resource(
        resource_name=resource_name,
        argument_resource=argument_resource,
    )

    # LOADING
    dataframe = load_data(path_resource)

    # TRANSFORMATION
    # -- Filtering information
    dataframe = filtering_data(resource_name, dataframe)

    # -- Extract nodes and edges
    nodes, edges = [], []
    adapter = extract_nodes_edges_ontoweaver(
        resource_name,
        dataframe,
    )

    nodes += adapter.nodes
    edges += adapter.edges

    # -- Fuse nodes, edges and write script for importing to Neo4j
    import_file = fuse_and_write(nodes, edges)
    logging.info(f"Processed {resource_name}: {len(nodes)} nodes, {len(edges)} edges.")


def resources_to_process(cli_arguments: argparse.Namespace) -> Dict[str, Any]:
    resource_mapping = {
        key: value
        for key, value in vars(cli_arguments).items()
        if value is not None and key != "verbose"
    }

    return resource_mapping


# ---------------------------------------------------------------------------
# ----------------------    M A I N   F U N T I O N    ----------------------
# ---------------------------------------------------------------------------
def main():

    # Parse CLI arguments
    asked = parse_arguments()

    # Configure logging settings
    setup_logging(asked.verbose)

    # Map the resources to be processed based on CLI arguments
    resource_mapping = resources_to_process(cli_arguments=asked)

    # Process the resources (ELT or ETL)
    for resource_name, argument_resource in resource_mapping.items():
        process_resource(resource_name, argument_resource)


if __name__ == "__main__":
    main()


# Example profiling:  poetry run python -m cProfile -s time weave_knowledge_graph_backup.py -net ./data_testing/networks/subset_interactions_edgecases.tsv
