from neo4j import GraphDatabase
from neo4j_config import NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD
class GraphDbManager:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWORD))

    def close(self):
        self.driver.close()

    def execute_query(self, query):
        with self.driver.session() as session:
            result = session.run(query)
        return result
