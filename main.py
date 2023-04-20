from spark_operations import SparkOperations
from logs_management import Logger
from spark_config import get_spark_ui_url
from config import RDF_DATA_PATH, RDF_EN_FR_TRANSFORMED_PATH, RDF_EN_FR_FILENAME, PREDICATES_TEMPLATE_PATH, EXPORTS_FOLDER_PATH


def extract_en_fr(input_file, output_file):
    # Old en-fr extraction
    # RDF_FILENAME = "freebase-rdf-latest.csv"
    # df = pd.read_csv(RDF_DATA_PATH + RDF_FILENAME)
    # extract_en_fr(input_file, output_file)

    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        for line in infile:
            if '@en' in line or '@fr' in line:
                outfile.write(line)
    print("[extract_en_fr] : Done.")


if __name__ == '__main__':
    # Initialisation of the Class performing all the Spark operations
    sparkOps = SparkOperations(
        app_name="TriplesRDF",
        RDF_DATA_PATH=RDF_DATA_PATH
    )

    # Initialisation of the logger object
    logger = Logger()

    url = get_spark_ui_url(sparkOps.sparkSession)

    input_file = RDF_DATA_PATH + RDF_EN_FR_FILENAME
    output_path = RDF_DATA_PATH + RDF_EN_FR_TRANSFORMED_PATH

    exportConfig = {
        "exportFullData": False,
        "exportUniquePredicates": False,
        "exportMatchingTriples": False,
        "exportSampleEnabled": True,
        "domainToExport": "computer",
        "exportSize": 0.3,
        "sample_output_folderpath": EXPORTS_FOLDER_PATH
    }

    logger.start_timer("processing")
    logger.log("Running Spark transformation and sampling domain", exportConfig["domainToExport"], "...")
    logger.log(f"Spark UI URL: {url}")

    operationsLogs = sparkOps.RDF_transform_and_sample_by_domain(
        input_file=input_file,
        output_path=output_path,
        exportConfig=exportConfig,
        performCounts=False,
        setLogToInfo=False,
        stopSession=False
    )

    logger.log("===== Spark transformation done =====")
    logger.stop_timer("processing")
    logger.log(operationsLogs)
