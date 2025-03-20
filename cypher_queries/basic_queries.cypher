
/*
QUERY 1: count the number of isolated nodes in the graph
*/
MATCH (n)
WHERE NOT (n)--()
RETURN count(n) AS isolated_nodes


/*
QUERY 2: count the number of relationships where the source and target are the same
*/
MATCH (n)-[:ProteinProteinInteraction]->(n) 
RETURN count(n) AS same_source_target_count


/*
QUERY 3: retrieves the source and target nodes involved in relationships where the is_stimulation property is equal to 1
*/
MATCH (source)-[r:ProteinProteinInteraction]->(target)
WHERE r.is_stimulation = 1
RETURN source, target
