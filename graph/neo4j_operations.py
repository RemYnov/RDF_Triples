import json
from logs_management import Logger


class GraphOperations:
    """
    Every operation needed for the graph modelisations and / or
    the data préparation / discovery will be managed by this class
    """
    def __init__(self):
        self.logger = Logger(prefix="- graph -")

    def filter_processed_data(self, depth=5):

        return "Hey"