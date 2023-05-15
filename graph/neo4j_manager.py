from neo4j import GraphDatabase
from neo4j_config import NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD

class GraphDbManager:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWORD))
        print('init ok')

    def close(self):
        self.driver.close()

    def execute_query(self, database, query, parameters=None):
        if parameters is None:
            parameters = {}

        with self._driver.session(database=database) as session:
            result = session.run(query, parameters)
            return result.values()
