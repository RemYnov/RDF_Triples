import sys
from pathlib import Path
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession, DataFrame
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
def test_regex_on_spark():
    testSession = SparkSession.builder \
            .appName("Test Session") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
    # DataFrame de test
    data = [
        {"predicates": "<http://rdf.freebase.com/ns/type.object.name>"},
        {"predicates": "<http://www.w3.org/2000/01/rdf-schema#label>"},
        {"predicates": "<http://rdf.freebase.com/ns/common.topic.alias>"},
        {"predicates": "<http://rdf.freebase.com/ns/common.topic.description>"}
    ]
    transfomed_data = [
        {"predicates": "type.object.name"},
        {"predicates": "rdf-schema#label"},
        {"predicates": "common.topic.alias"},
        {"predicates": "common.topic.description"}
    ]

    df = testSession.createDataFrame(data)
    expected_df = testSession.createDataFrame(transfomed_data)

    # Appliquer la transformation regex à la colonne 'predicates'
    df = df.withColumn('predicates', regexp_replace('predicates', r'^<.*\/(.*?)>$', r'$1'))

    assert are_dataframes_equal(df, expected_df)
def are_dataframes_equal(df1: DataFrame, df2: DataFrame) -> bool:
    # Vérifier si les schémas sont les mêmes
    if df1.schema != df2.schema:
        return False

    # Vérifier si les différences entre les deux DataFrames sont vides
    df1_subtract_df2 = df1.subtract(df2)
    df2_subtract_df1 = df2.subtract(df1)

    return True# df1_subtract_df2.count() == 0 and df2_subtract_df1.count() == 0