#!/usr/bin/env python3

"""
Script Name: Omnipath Adapter (secondary type)
Description:
    This script retrieve information from Omnipath databases

Usage:
    Example 1:
    poetry run python weave_knowledge_graph_backup.py /
    -net from-cache

    Example 2:
    poetry run python weave_knowledge_graph.py \ 
    -net ./data_testing/networks/subset_interactions_edgecases.tsv

Arguments:
    -net, --networks        Path.
    -enz, --enzyme-PTM      Path to the output report file.
    -co, --complexes        Path.
    -an, --annotations      Path.
    -inter, --intercell
    -v, --verbose

"""


import argparse
import logging
import pandas as pd
import yaml


import ontoweaver
from biocypher import BioCypher
from biocypher._get import Downloader, FileDownload

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


# ----------------------    FUNCTIONS    ----------------------
def parse_arguments():
    usage = f"Extract nodes and edges from CSV tables of Omnipath database: networks, enzyme-PTM, complexes, annotations and intercell."

    parser = argparse.ArgumentParser(description=usage)

    parser.add_argument(
        "-net",
        "--networks",
        metavar="TSV",
        nargs="+",
        help="Extract from the Omnipath 'networks' TSV file.",
    )

    parser.add_argument(
        "-enz",
        "--enzyme-PTM",
        metavar="TSV",
        nargs="+",
        help="Extract from the Omnipath 'enzyme-PTM' TSV file.",
    )

    parser.add_argument(
        "-co",
        "--complexes",
        metavar="TSV",
        nargs="+",
        help="Extract from the Omnipath 'complexes' TSV file.",
    )

    parser.add_argument(
        "-an",
        "--annotations",
        metavar="TSV",
        nargs="+",
        help="Extract from the Omnipath 'annotations' TSV file.",
    )

    parser.add_argument(
        "-inter",
        "--intercell",
        metavar="TSV",
        nargs="+",
        help="Extract from the Omnipath 'intercell' TSV file.",
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

    return parser.parse_args()


def download_resources(url_resource):
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


def setup_logging(level):
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


def main():

    # Parse CLI arguments
    asked = parse_arguments()

    # Configure logging settings
    setup_logging(asked.verbose)

    # Instanciate BioCypher with the YAML file information (schema and biocypher config)
    bc = initialize_biocypher()

    # Current graph data
    nodes, edges = [], []

    if asked.networks:

        # Extract nodes and edges from the TSV file
        if asked.networks[0] == "from-cache":
            path_networks = download_resources(url_resource=URL_OMNIPATH_NETWORKS_LATEST)
            extracted_nodes, extracted_edges = extract_networks(path_networks[0])
        else:
            extracted_nodes, extracted_edges = extract_networks(asked.networks[0])

        nodes += extracted_nodes
        edges += extracted_edges
        logging.info(f"Wove Networks: {len(nodes)} nodes, {len(edges)} edges.")

        import_file = fuse_and_write(nodes, edges)


if __name__ == "__main__":

    main()

# Example of usage in CLI:
# poetry run python weave_knowledge_graph.py -net ./data_testing/networks/subset_interactions_edgecases.tsv
