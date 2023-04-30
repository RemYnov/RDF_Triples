from logs_management import Logger

class RDF_Graph_Model:
    """
    Every spark operations will be managed and monitored
    from this class.
    """

    def __init__(self, model_name):
        self.logger = Logger(prefix="- graph modeling -")
