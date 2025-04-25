
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

/*
QUERY 4: count the number of edges in the graph
*/
MATCH ()-[r]->()
RETURN COUNT(r) as total_edges

/*
QUERY 5: count the number of edges in the graph, that don't have some databases as sources properties.
*/
WITH ['Wang', 'SPIKE', 'SPIKE_LC'] AS forbiddenSources
MATCH ()-[r]->()
WITH apoc.text.split(r.sources, ';') AS sourcesArray, forbiddenSources, r
WHERE NONE(source IN sourcesArray WHERE source IN forbiddenSources)
RETURN COUNT(r) AS count

/*
QUERY 6: count the number of edges in the graph, with more than 3 different sources as properties.
*/
WITH ['Wang', 'SPIKE', 'SPIKE_LC'] AS forbiddenSources
MATCH ()-[r]->()
WITH r, apoc.text.split(r.sources, ';') AS sourcesArray
    // Remove elements from forbiddenSources
WHERE NONE(element IN sourcesArray WHERE element IN forbiddenSources)
    // Ensure there are more than 3 unique sources
WITH r, size([source IN sourcesArray | source]) AS uniqueSourcesCount
WHERE uniqueSourcesCount > 3
RETURN COUNT(r) AS count
