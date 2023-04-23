import pandas as pd
from neo4j_config import CSV_PATH

class CypherGenerator:
    """
    This class will generate all the Cypher query that will be executed to load
    the RDF datas to Neo4j, with the good relationships and properties.
    (based on the RDF_Graph_Model objects)
    """
    @staticmethod
    def from_csv(file_name):
        csv_file_path = f"{CSV_PATH}/{file_name}"
        df = pd.read_csv(csv_file_path)
        return CypherGenerator.from_dataframe(df)

    @staticmethod
    def from_dataframe(df):
        # Générez les requêtes Cypher en fonction de la structure du DataFrame
        # ...
        cypher_queries = ""
        return cypher_queries
