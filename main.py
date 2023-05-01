from processing.spark_operations import SparkOperations
from pyspark.sql import SparkSession
from logs_management import Logger, global_exception_handler
from processing.spark_config import get_spark_ui_url
from config import RDF_DATA_PATH, RDF_EN_FR_TRANSFORMED_PATH, RDF_EN_FR_FILENAME, EXPORTS_FOLDER_PATH, SPARK_UI_URL
import json
import sys


if __name__ == '__main__':
    RUN_NAME = "50k full NLP pipeline (with new pattern) externalized from RDF_transform()"
    # Initialisation of the logger object and the exception handler
    logger = Logger(defaultCustomLogs="fancy", botEnabled=True, runName=RUN_NAME)
    sys.excepthook = lambda et, ev, tb: global_exception_handler(logger, et, ev, tb)  # For unexpected error

    logger.log("==Running Spark transformation==")

    # Initialisation of the Class performing all the Spark operations
    sparkOps = SparkOperations(
        app_name="TriplesRDF",
        RDF_DATA_PATH=RDF_DATA_PATH,
        botLoggerEnabled=True
    )

    url = get_spark_ui_url(sparkOps.sparkSession)

    input_file = RDF_DATA_PATH + RDF_EN_FR_FILENAME
    output_path = RDF_DATA_PATH + RDF_EN_FR_TRANSFORMED_PATH

    exportConfig = {
        "exportUniquePredicates": False,
        "exportMatchingTriples": False,
        "exportSampleEnabled": False,
        "exportFullData": False,
        "exportFullPath": output_path,
        "domainToExport": "common",
        "exportSize": 0.5,
        "sample_output_folderpath": EXPORTS_FOLDER_PATH
    }

    logger.start_timer("Spark processing")
    logger.log(f"Spark UI URL: {url} or {SPARK_UI_URL}")

    operationsLogs, df_RDF = sparkOps.RDF_transform_and_sample_by_domain(
        input_file=input_file,
        exportConfig=exportConfig,
        performCounts=False,
        setLogToInfo=False,
        stopSession=False,
        showSample=False
    )

    logger.log("==Spark transformation over==")
    logger.stop_timer("Spark processing")
    logger.log(json.dumps(operationsLogs, indent=4, sort_keys=False, separators=(',', ': ')))

    logger.log("==Searching for related Triples==")
    related_subjects, matchingLogs = sparkOps.find_matching_triples(main_df=df_RDF)
    logger.log("==Matching Over==")
    logger.log(json.dumps(matchingLogs, indent=4, sort_keys=False, separators=(',', ': ')))

    result_path = RDF_DATA_PATH + "sparkedData/fullExploResults/matchingTriples"
    csv_path = RDF_DATA_PATH + "sparkedData/fullExploResults/matchingTriples_csv"

    sparkOps.parquet_reading(result_path, csv_file_path="")

    logger.log(RUN_NAME + " END.", isTitle=True)


def monTest():
    data = [("g.11bbql90n_", "common.notable_for.display_name", "Personnage de film"),
            ("g.11b75s88wg", "common.notable_for.display_name", "Produit de grande consommation"),
            ("g.1254y8qqv", "common.notable_for.display_name", "Édition de livre"),
            ("g.11b5lw8t_m", "common.notable_for.display_name", "épisode de Série Télévisée")]

    columns = ["Subject", "Predicate", "Object"]

    sparkTest = SparkOperations(
        app_name="TriplesRDF",
        RDF_DATA_PATH=RDF_DATA_PATH,
        botLoggerEnabled=True
    )

    df = sparkTest.sparkSession.createDataFrame(data, columns)
    token = sparkTest.apply_NLP_pipeline(df)

    token.show(25, truncate=False)

    token_df = token.toPandas()

#monTest()



