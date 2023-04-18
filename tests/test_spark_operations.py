import re
from pyspark.sql import SparkSession
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from spark_operations import SparkOperations

def test_transform_predicate():
    raw_predicates = ["<http://rdf.freebase.com/ns/type.object.name>",
                      "<http://rdf.freebase.com/ns/common.notable_for.display_name>",
                      "<http://www.w3.org/2000/01/rdf-schema#label>"]

    expected_predicates = ["type.object.name",
                          "common.notable_for.display_name",
                          "type.object.name"]
    for i in range(0, len(raw_predicates)):
        transformed_predicte = SparkOperations.transform_predicate(raw_predicates[i])
        assert transformed_predicte == expected_predicates[i]

def test_transform_subject():
    raw_subjects = ["<http://rdf.freebase.com/ns/american_football.football_player.footballdb_id>",
                    "<http://rdf.freebase.com/ns/astronomy.astronomical_observatory.discoveries>",
                    "<http://rdf.freebase.com/ns/biology.organism_classification.child_classifications>"]

    expected_subjects = ["american_football.football_player.footballdb_id",
                         "astronomy.astronomical_observatory.discoveries",
                         "biology.organism_classification.child_classifications"]

    for i in range(0, len(raw_subjects)):
        transformed_predicte = SparkOperations.transform_predicate(raw_subjects[i])
        assert transformed_predicte == expected_subjects[i]

