MATCH (a)-[]->(b)-[]->(c) WHERE a.span.start_time_unix_nano > 5 RETURN a.service.name
