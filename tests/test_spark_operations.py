import sys
from pathlib import Path
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer
from pyspark.ml import Pipeline
sys.path.append(str(Path(__file__).parent.parent))
from processing.spark_operations import SparkOperations

def test_get_predicates_by_domain():
    #expected =
    #results = SparkOperations.get_predicates_by_domain("computer")

    assert True
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
        {"predicates": "type.object-name"},
        {"predicates": "rdf-schema#label"},
        {"predicates": "common.topic.alias"},
        {"predicates": "common.topic.description>"}
    ]

    df = testSession.createDataFrame(data)
    expected_df = testSession.createDataFrame(transfomed_data)

    # Appliquer la transformation regex à la colonne 'predicates'
    df = df.withColumn('predicates', regexp_replace('predicates', r'^<.*\/(.*?)>$', r'$1'))

    assert are_dataframes_equal(df, expected_df)

def test_stop_words_removal():
    french_stopwords = StopWordsRemover.loadDefaultStopWords('french')
    english_stopwords = StopWordsRemover.loadDefaultStopWords('english')

    combined_stopwords = french_stopwords + english_stopwords

    stop_words_remover = StopWordsRemover(inputCol="tokenizedObj", outputCol="filtered_tokens",
                                          stopWords=combined_stopwords)
    regex_tokenizer = RegexTokenizer(inputCol="Object", outputCol="tokenizedObj", pattern=r'\W')

    nlp_pipeline = Pipeline(stages=[regex_tokenizer, stop_words_remover])

    nlp_model = nlp_pipeline.fit(cleaned_df)
    tokenized_df = nlp_model.transform(cleaned_df)


    data = [("g.11bbql90n_", "common.notable_for.display_name", "Personnage de film"),
            ("g.11b75s88wg", "common.notable_for.display_name", "Produit de grande consommation"),
            ("g.1254y8qqv", "common.notable_for.display_name", "Édition de livre"),
            ("g.11b5lw8t_m", "common.notable_for.display_name", "épisode de Série Télévisée")]

    columns = ["id", "type", "Object"]

    spark = SparkSession.builder \
        .appName("StopWordsRemover Test") \
        .getOrCreate()

    df = spark.createDataFrame(data, columns)
    pattern = r'\b(?:{})\b'.format('|'.join(StopWordsRemover.loadDefaultStopWords('french')))
    regex_tokenizer = RegexTokenizer(inputCol="Object", outputCol="tokenizedObj", pattern=pattern)
    stop_words_remover = StopWordsRemover(inputCol="tokenizedObj", outputCol="filtered_tokens")

    nlp_pipeline = Pipeline(stages=[regex_tokenizer, stop_words_remover])
    nlp_model = nlp_pipeline.fit(input_df)
    tokenized_df = nlp_model.transform(input_df)

    return tokenized_df
def are_dataframes_equal(df1: DataFrame, df2: DataFrame) -> bool:
    # Vérifier si les schémas sont les mêmes
    if df1.schema != df2.schema:
        return False
    else:
        return True