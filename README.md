# Omnipath Secondary Adapter

**Description:**

The main goal of this adapter is to provide user the ability to retrieve data from the [Omnipath Database](https://omnipathdb.org/) and use this data with BioCypher. For achieving this goal we are going to built the adapter with [OntoWeaver](https://github.com/oncodash/ontoweaver).


## Steps to create an adapter with Ontoweaver
1. Create a folder to store the data in our project. This folder can called `data`.
2. Add the folder `data` to the `.gitignore` file to avoid uploading large datasets in our repository.
3. [**TO DO**] If there is a step of preprocessing, create `weave_knowledge_graph.py` following the ontoweaver python template. If not, use the CLI command in the new version.

4. Create an Ontoweaver mapping file (`.yaml`) in the folder `/omnipath_secondary_adapter/adapters`. You should create one Ontoweaver mapping file for each table you are interested in use for building your knowledge graph. The purpose of each file is to map columns from each table with graph entities (nodes and relationships). At the same time, you can apply [Ontoweaver Transformers](https://ontoweaver.readthedocs.io/en/latest/readme_sections/mapping_api.html#available-transformers) to the data before use the data.

5. Create a BioCypher schema file  (`schema_config.yaml`) in the folder `/config/`. This file will serve to define which nodes and relationships will be present in our graph. For more information [click here](https://biocypher.org/quickstart.html#the-schema-configuration-yaml-file)

> [!IMPORTANT]
> Remember: You should create one single `schema_config_file.yaml`. On the other hand, you should create one ontoweaver YAML file per table you are using.

6. Add an ontology to be based on in `config/biocypher_config.yaml` and refer the entities and associations of the ontology in the `config/schema_config.yaml`.

## Not to forget

- For each column that you want extract, you need ot have (at least) one transformer.
- 'label' == 'node|edge type'
- underscore are necessary in labels in OntoWeaver adapters

7. Complete the exercice: Add the properties to the schema and the mapping files
8. Extract the data
9. Visualise in neo4j

## TO DO

10. Check if the graph is consistent with the tabular data.
    - Possible tests:
      - Verify there is not error when a source and a target are the same.
      - Count the number of edges in total.
      - Count the number of nodes in total.
      - Count the number of nodes by group.
      - Test if the schema is correct.
11. Fix the Ontoweaver dependency (pip library instead folder in the project)

# Frequently Asked Questions

1. **What does it mean "secondary adapter"?** -- it means the data in the adapter has been the result of harmonized procedures and does not constitute the primary source of information. For instance: the omnipath interactions table has been built by using more than one original resource (other tables).
