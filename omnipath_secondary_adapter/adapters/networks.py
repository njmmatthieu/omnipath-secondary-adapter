import ontoweaver.base as base
import ontoweaver.tabular as tabular
import ontoweaver.types as types
import ontoweaver.validate as validate

class OmniPath(base.Transformer):
    """Custom end-user transformer, used to create elements for OmniPath KG database."""

    def __init__(self, properties_of, value_maker = None, label_maker = None, branching_properties = None, columns=None, output_validator: validate.OutputValidator = None, multi_type_dict = None, raise_errors = True, **kwargs):

        super().__init__(properties_of, value_maker, label_maker, branching_properties, columns, output_validator,
                         multi_type_dict, raise_errors=raise_errors, **kwargs)

    def __call__(self, row, i):

        self.final_type = None
        self.properties_of = None

        # FIXME: Should be in base.Transformer. Here due to circular inheritance issues.
        a = tabular.Declare()

        # First declare all nodes and edges used in the branching logic.

        target_protein = a.make_node_class("target_protein", self.branching_properties.get("target_protein", {}))
        target_complex = a.make_node_class("target_complex", self.branching_properties.get("target_complex", {}))

        possible_sources = ["protein", "source_protein", "mirna", "lncrna", "drug", "macromolecular_complex"]

        for possible_source in possible_sources:
            a.make_edge_class("transcriptional", getattr(types, possible_source), getattr(types, "protein"), self.branching_properties.get("transcriptional", {}))
            a.make_edge_class("transcriptional", getattr(types, possible_source), getattr(types, "macromolecular_complex"), self.branching_properties.get("transcriptional", {}))
            a.make_edge_class("transcriptional", getattr(types, possible_source), getattr(types, "mirna"), self.branching_properties.get("transcriptional", {}))
            a.make_edge_class("post_translational", getattr(types, possible_source), getattr(types, "protein"), self.branching_properties.get("post_translational", {}))
            a.make_edge_class("post_translational", getattr(types, possible_source), getattr(types, "macromolecular_complex"), self.branching_properties.get("post_translational", {}))
            a.make_edge_class("post_transcriptional", getattr(types, possible_source), getattr(types, "protein"), self.branching_properties.get("post_transcriptional", {}))
            a.make_edge_class("drug_has_target", getattr(types, possible_source), getattr(types, "protein"), self.branching_properties.get("drug_has_target", {}))
            a.make_edge_class("drug_has_target", getattr(types, possible_source), getattr(types, "macromolecular_complex"), self.branching_properties.get("drug_has_target", {}))
            a.make_edge_class("mirna_transcriptional", getattr(types, possible_source), getattr(types, "mirna"), self.branching_properties.get("mirna_transcriptional", {}))
            a.make_edge_class("lncrna_post_transcriptional", getattr(types, possible_source), getattr(types, "protein"), self.branching_properties.get("lncrna_post_transcriptional", {}))

        # Extract branching information from the current row, as well as node ID.

        node_id = row["target"]
        type = row["type"]
        entity = row["entity_type_target"]

        # Create branching logic and return correct elements.

        if type == "transcriptional":
            if entity == "protein":
                self.final_type = getattr(types, "protein")
                self.properties_of = self.branching_properties.get("target_protein", {})
                yield node_id, getattr(types, "transcriptional"), target_protein, None

            elif entity == "complex":
                self.final_type = getattr(types, "macromolecular_complex")
                self.properties_of = self.branching_properties.get("target_complex", {})
                yield node_id,  getattr(types, "transcriptional"), target_complex, None

            elif entity == "mirna":
                self.properties_of = self.branching_properties.get("mirna", {})
                yield node_id,   getattr(types, "transcriptional"), getattr(types, "mirna"), None


        elif type == "post_translational":
            if entity == "protein":
                self.final_type = getattr(types, "protein")
                self.properties_of = self.branching_properties.get("target_protein", {})
                yield node_id, getattr(types, "post_translational"), target_protein, None

            elif entity == "complex":
                self.final_type = getattr(types, "macromolecular_complex")
                self.properties_of = self.branching_properties.get("target_complex", {})
                yield node_id, getattr(types, "post_translational"), target_complex, None


        elif type == "post_transcriptional":
            self.final_type = getattr(types, "protein")
            self.properties_of = self.branching_properties.get("target_protein", {})
            yield node_id, getattr(types, "post_transcriptional"), target_protein, None

        elif type == "small_molecule_protein":
            # Note: your YAML uses keys "protein" and "complex" without comparison
            if entity == "protein":
                self.final_type = getattr(types, "protein")
                self.properties_of = self.branching_properties.get("target_protein", {})
                yield node_id, getattr(types, "drug_has_target"), target_protein, None

            elif entity == "complex":
                self.final_type = getattr(types, "macromolecular_complex")
                self.properties_of = self.branching_properties.get("target_complex", {})
                yield node_id, getattr(types, "drug_has_target"), target_complex, None

        elif type == "mirna_transcriptional":
            self.properties_of = self.branching_properties.get("mirna", {})
            yield node_id, getattr(types, "mirna_transcriptional"), getattr(types, "mirna"), None

        elif type == "lncrna_post_transcriptional":
            self.final_type = getattr(types, "protein")
            self.properties_of = self.branching_properties.get("target_protein", {})
            yield node_id, getattr(types, "lncrna_post_transcriptional"), target_protein, None
