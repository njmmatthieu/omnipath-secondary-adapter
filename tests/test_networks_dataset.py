import os
import subprocess
import yaml
import pytest

import pandas as pd
from datetime import datetime
from ontoweaver.exceptions import DataValidationError


# ----------------------------------------   CONSTANTS    -------------------------------------
SCHEMA_FILE = "config/schema_config.yaml"
ONTOWEAVER_SCHEMA_FILES = ["networks.yaml"]

BIOCYPHER_OUTPUT_FOLDER = "./biocypher-out"

TESTING_DATASET_INTERACTIONS = "networks/subset_interactions_edgecases.tsv"


# -------------------------------------   HELPER FUNCTIONS  -----------------------------------
def check_file_exists_and_not_empty(file_path):
    """Function to check if a file exists and is not empty."""
    return os.path.isfile(file_path) and os.path.getsize(file_path) > 0


def check_folder_exists_and_not_empty(folder_path):
    """Function to check if a folder exists and is not empty."""
    return os.path.exists(folder_path) and any(os.listdir(folder_path))


def extract_node_properties(schema_file: str, node_type: str):
    with open(schema_file, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

    # The first header's colum is :ID
    columns = [":ID"]

    # Extract properties of 'node' as columns
    node_properties = data.get(node_type, {}).get("properties", {})

    for key, value in node_properties.items():
        if value == "int" or value == "integer":
            columns.append(key + ":long")
        else:
            columns.append(key)

    # Extend columns to the current list of properties
    columns.extend(["id", "preferred_id", ":LABEL"])

    return columns


def open_latest_folder(base_path: str):
    """
    Finds and opens the latest folder based on date format 'YYYYMMDDHHMMSS'.

    :param base_path: The directory containing the dated folders
    :return: The path of the latest folder or None if no valid folders exist
    """
    folders = [
        f
        for f in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, f)) and f.isdigit()
    ]

    # Convert folder names to datetime objects
    valid_folders = []
    for folder in folders:
        try:
            folder_date = datetime.strptime(folder, "%Y%m%d%H%M%S")
            valid_folders.append((folder_date, folder))
        except ValueError:
            continue  # Skip invalid folder names

    if not valid_folders:
        return None

    # Get the latest folder by date
    latest_folder = max(valid_folders, key=lambda x: x[0])[1]
    latest_folder_path = os.path.join(base_path, latest_folder)

    return latest_folder_path


def extract_header_from_csv(folder_path, file_path):
    csv_file_path = os.path.join(folder_path, file_path)
    df = pd.read_csv(csv_file_path, sep=None, engine="python")

    header = df.columns.to_list()

    return header


def extract_unique_nodes_original_dataframe(dataset_filepath):
    df = pd.read_csv(dataset_filepath, sep=None, engine="python")

    unique_nodes = pd.concat([df["source"], df["target"]]).unique()

    return unique_nodes


def extract_unique_nodes_biocypher_dataframe(dataset_filepath):
    df = pd.read_csv(dataset_filepath, header=None, sep=None, engine="python")

    unique_nodes = df.iloc[:, 0]

    print(unique_nodes)

    return unique_nodes


def extract_edge_properties(schema_file: str, edge_type: str):
    with open(schema_file, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

    # The first header's colum is :ID
    columns = [":START_ID", "id"]

    # Extract properties of 'node' as columns
    edge_properties = data.get(edge_type, {}).get("properties", {})

    for key, value in edge_properties.items():
        if value == "int" or value == "integer":
            columns.append(key + ":long")
        else:
            columns.append(key)

    # Extend columns to the current list of properties
    columns.extend([":END_ID", ":TYPE"])

    return columns


def extract_unique_edges_biocypher_dataframe(dataset_filepath):
    df = pd.read_csv(dataset_filepath, header=None, sep=None, engine="python")

    unique_edges = df.iloc[:, 0]

    print(unique_edges)

    return unique_edges


# ----------------------------------------------------------------------------------
# ---------------------------------    T E S T S   ---------------------------------
# ----------------------------------------------------------------------------------
# ---- General tests
# TODO: check if the BioCypher schema file exist and it not empty
def test_biocypher_schema_exists():
    file_path = SCHEMA_FILE
    assert check_file_exists_and_not_empty(file_path)


# TODO: check if the OntoWever schema files exist and they are not empty
def test_ontoweaver_schemas_exists():
    adapter_path = os.path.join("omnipath_secondary_adapter", "adapters")

    for schema in ONTOWEAVER_SCHEMA_FILES:
        schema_path = os.path.join(adapter_path, schema)
        assert check_file_exists_and_not_empty(schema_path)


# ----- Corner Cases
# TODO: verify header in the CSV related to nodes
def test_run_ontoweaver_script_networks():
    # Run the weaver script
    command = [
        "poetry",
        "run",
        "python",
        "weave_knowledge_graph.py",
        "-net",
        "./data_testing/networks/subset_interactions_edgecases.tsv",
    ]

    result = subprocess.run(command, capture_output=True, text=False)

    assert result.returncode == 0, f"Command failed with error: {result.stderr}"


# TODO: verify if the biocypher output folder exist
def test_biocypher_output_folder():
    biocypher_output_folder = BIOCYPHER_OUTPUT_FOLDER
    assert check_folder_exists_and_not_empty(biocypher_output_folder)


# TODO: verify if the header for nodes is as expected
def test_csv_header_nodes():

    # Extract node properties as header
    expected_header = extract_node_properties(
        schema_file=SCHEMA_FILE,
        node_type="protein",
    )

    # Read header from current file from last folder generated
    last_folder = open_latest_folder(BIOCYPHER_OUTPUT_FOLDER)
    print(last_folder)

    actual_header = extract_header_from_csv(last_folder, "Protein-header.csv")

    assert expected_header == actual_header


def test_number_nodes():
    # Expected number of nodes
    original_dataset_filepath = os.path.join("data_testing", TESTING_DATASET_INTERACTIONS)
    expected_number_nodes = len(
        extract_unique_nodes_original_dataframe(original_dataset_filepath)
    )

    # Actual number of nodes
    last_folder = open_latest_folder(BIOCYPHER_OUTPUT_FOLDER)
    biocypher_nodes_file = os.path.join(last_folder, "Protein-part000.csv")
    actual_number_nodes = len(
        extract_unique_nodes_biocypher_dataframe(biocypher_nodes_file)
    )

    assert (
        expected_number_nodes == actual_number_nodes
    ), f"Mismatch number of nodes between testing dataset and file generated by BioCypher"


# TODO: verify header in the CSV related to relationships
def test_csv_header_edges():

    # Extract node properties as header
    expected_header = extract_edge_properties(
        schema_file=SCHEMA_FILE,
        edge_type="protein protein interaction",
    )

    # Read header from current file from last folder generated
    last_folder = open_latest_folder(BIOCYPHER_OUTPUT_FOLDER)
    print(last_folder)

    actual_header = extract_header_from_csv(
        last_folder, "ProteinProteinInteraction-header.csv"
    )

    assert expected_header == actual_header


# TODO: number of relationships are 10
def test_number_edges():
    # Expected number of edges
    expected_number_edges = 10

    # Current number of edges
    # Actual number of nodes
    last_folder = open_latest_folder(BIOCYPHER_OUTPUT_FOLDER)
    biocypher_edges_file = os.path.join(
        last_folder, "ProteinProteinInteraction-part000.csv"
    )
    actual_number_edges = len(
        extract_unique_edges_biocypher_dataframe(biocypher_edges_file)
    )

    assert expected_number_edges == actual_number_edges


class MyCustomError(Exception):
    pass


# TODO: verify the process stop when found missing source/target
def test_missing_source():
    # Run the weaver script
    command = [
        "poetry",
        "run",
        "python",
        "weave_knowledge_graph.py",
        "-net",
        "./data_testing/networks/subset_interactions_missing_source.tsv",
    ]

    with pytest.raises(subprocess.CalledProcessError) as excinfo:
        result = subprocess.run(command, check=True, capture_output=True, text=True)

    assert (
        "DataValidationError" in excinfo.value.stderr
    ), f"Expected 'DataValidationError' in stderr, but got: {excinfo.value.stderr}"


def test_missing_target():
    # Run the weaver script
    command = [
        "poetry",
        "run",
        "python",
        "weave_knowledge_graph.py",
        "-net",
        "./data_testing/networks/subset_interactions_missing_target.tsv",
    ]

    with pytest.raises(subprocess.CalledProcessError) as excinfo:
        result = subprocess.run(command, check=True, capture_output=True, text=True)

    assert (
        "DataValidationError" in excinfo.value.stderr
    ), f"Expected 'DataValidationError' in stderr, but got: {excinfo.value.stderr}"


# TODO: verify properties are case sensitive, and none of those properties are discarded


# TODO: verify the Neo4 script runs normally
