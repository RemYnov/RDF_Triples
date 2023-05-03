from logs_management import Logger

class RDF_Graph_Model:
    """
    Instances from this class will be based on a given sample of RDF.
    From this sample, we will compute a Graph Model that will be used in the
    CypherGenerator class to build the query that will load data to Neo4j
    """

    def __init__(self, model_name="RDF Graph"):
        self.logger = Logger(prefix="- graph modeling -")

        self.min_similarity_rate = 0.51
        self.max_similarity_rate = 0.99
        self.nodes = []
        self.edges = []
