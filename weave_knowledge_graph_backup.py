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

import pandas as pd
import yaml
from biocypher import BioCypher
from biocypher._get import Downloader, FileDownload

import ontoweaver

# ----------------------    CONSTANTS    ----------------------

CACHE_DIRECTORY = "./data"

URL_OMNIPATH_NETWORKS_LATEST = (
    "https://archive.omnipathdb.org/omnipath_webservice_interactions__latest.tsv.gz"
)
URL_OMNIPATH_ANNOTATIONS_LATEST = (
    "https://archive.omnipathdb.org/omnipath_webservice_annotations__latest.tsv.gz"
)
URL_OMNIPATH_ENZPTM_LATEST = (
    "https://archive.omnipathdb.org/omnipath_webservice_enz_sub__latest.tsv.gz"
)
URL_OMNIPATH_INTERCELL_LATEST = (
    "https://archive.omnipathdb.org/omnipath_webservice_intercell__latest.tsv.gz"
)

URL_OMNIPATH_COMPLEXES_LATEST = (
    "https://archive.omnipathdb.org/omnipath_webservice_complexes__latest.tsv.gz"
)


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

    return parser.parse_args()


def download_resources(url_resource: str) -> list:
    """Download the Omnipath dataset given an URL to the resource

    Args:
        url_resource (str): URL to the Omnipath dataset

    Returns:
        list: list containing the directories where the dataset is stored.
    """

    # Define the directory where the data will be store
    cache_directory = CACHE_DIRECTORY

    # Instanciate the Downloader
    downloader = Downloader(cache_dir=cache_directory)

    # Define the resource
    dataset = FileDownload(
        name="omnipath_archive",
        url_s=[url_resource],
        lifetime=7,  # Cache for 7 days
    )

    # Download the resource and store the paths where is stored
    paths = downloader.download(dataset)

    return paths


def setup_logging(level: int | str) -> None:
    """Configure logging."""
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")


def initialize_biocypher():
    """Initialize BioCypher with configuration files."""
    return BioCypher(
        biocypher_config_path="config/biocypher_config.yaml",
        schema_config_path="config/schema_config.yaml",
    )


def extract_networks(networks_file):
    """Extract nodes and edges from Omnipath networks file."""
    logging.info("Weaving Omnipath networks data...")
    networks_df = pd.read_csv(networks_file, sep="\t")

    mapping_file = "./omnipath_secondary_adapter/adapters/networks.yaml"
    with open(mapping_file) as fd:
        mapping = yaml.full_load(fd)

    adapter = ontoweaver.tabular.extract_table(
        df=networks_df, config=mapping, separator=":", affix="none"
    )

    return adapter.nodes, adapter.edges


def fuse_and_write(nodes, edges):
    """Fuse duplicated nodes and edges and write the output."""
    return ontoweaver.reconciliate_write(
        nodes,
        edges,
        "config/biocypher_config.yaml",
        "config/schema_config.yaml",
        separator=", ",
    )


# ---------------------------------------------------------------------------
# ----------------------    M A I N   F U N T I O N    ----------------------
# ---------------------------------------------------------------------------
def main():

    # Parse CLI arguments
    asked = parse_arguments()

    # Configure logging settings
    setup_logging(asked.verbose)

    # Current graph data (empty in this point)
    nodes, edges = [], []

    print(asked)

    if asked.networks:

        print(asked.networks)

        # Extract nodes and edges from the TSV file
        if asked.networks == "download":
            path_networks = download_resources(url_resource=URL_OMNIPATH_NETWORKS_LATEST)
            extracted_nodes, extracted_edges = extract_networks(path_networks[0])
        else:
            extracted_nodes, extracted_edges = extract_networks(asked.networks)

        nodes += extracted_nodes
        edges += extracted_edges
        logging.info(f"\nInfo Networks: {len(nodes)} nodes, {len(edges)} edges.")

        import_file = fuse_and_write(nodes, edges)


if __name__ == "__main__":
    main()


# Example profiling:  poetry run python -m cProfile -s time weave_knowledge_graph.py -net ./data_testing/networks/subset_interactions_edgecases.tsv
