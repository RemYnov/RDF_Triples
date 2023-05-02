import json
from logs_management import Logger


class GraphOperations:
    """
    Every operation needed for the graph modelisations and / or
    the data préparation / discovery will be managed by this class
    """
    def __init__(self):
        self.logger = Logger(prefix="- graph -")


    def perform_data_discovery(self, df):
        """
        For a given dataframe storing matching triples, retrieve both every information
        of the current Subject and every information of every matched subjects.
        To study : the need of the predicate ? Use it for Priority ?
        :return: discovered_df, a dataframe having, additionally to the matching triples,
        every information of every subject involved in the match.
        """

        discovered_df = []
        return discovered_df
    def filter_processed_data(self, depth=5):
        """
        This function will allow us to make our graph a more or less detailed.
        We got more than 368M matches for 75k unique predicates, which is huge.
        To reduce the load of our graph, we need to filter the data based on several rules,
        criterias, to be defined
        :param depth: the number of matchs allowed for a unique token from a unique object
        :return: filtered_df, a dataframe lighter than the input one, thanks to the filtering
        performed on each unique Subject's tokens
        """
        filtered_df = []

        return filtered_df