MATCH (a)-[]->(b)-[]->(c) WHERE c.service.name = 'ratings-v1' RETURN a.service.name
