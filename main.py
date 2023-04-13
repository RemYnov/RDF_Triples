from spark_operations import SparkOperations
from spark_config import print_progress, get_spark_ui_url
import threading
import time

RDF_DATA_PATH = "D:/MEP/SANDBOX/data/"
RDF_FILENAME = "freebase-rdf-latest.csv"
RDF_EN_FR_FILENAME = "freebase-rdf-en-fr-latest.csv"
RDF_EN_FR_TRANSFORMED_PATH = "sparkedData/rdf_transfo"
PREDICATES_TEMPLATE_PATH = RDF_DATA_PATH + "test/backup_predicates_template.json"
EXPORTS_FOLDER_PATH = RDF_DATA_PATH + "sparkedData/exploResults/sample_"

RDF_EN_FR_SAMPLES = "rdf-en-fr_samples_3000000.csv"

def extract_en_fr(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        for line in infile:
            if '@en' in line or '@fr' in line:
                outfile.write(line)
    print("[extract_en_fr] : Done.")


if __name__ == '__main__':
    #df = pd.read_csv(RDF_DATA_PATH + RDF_FILENAME)
    #extract_en_fr(input_file, output_file)

    # Initialisation of the Class performing all the Spark operations
    sparkOps = SparkOperations(
        app_name="TriplesRDF",
        RDF_DATA_PATH=RDF_DATA_PATH,
        PREDICATES_TEMPLATE_PATH=PREDICATES_TEMPLATE_PATH
    )

    url = get_spark_ui_url(sparkOps.sparkSession)

    input_file = RDF_DATA_PATH + RDF_EN_FR_FILENAME
    output_path = RDF_DATA_PATH + RDF_EN_FR_TRANSFORMED_PATH
    domainToExport = "aviation"
    exportSize = 0.3

    print("Running Spark transformation and sampling domain", domainToExport, "...")
    print(f"Spark UI URL: {url}")

    start_time = time.time()
    # Créer et démarrer un thread pour afficher la barre de progression
    #t = threading.Thread(target=print_progress(sparkOps.context, 150), args=(sparkOps.context,))
    #t.start()

    operationsLogs = sparkOps.RDF_transform_and_sample_by_domain(
        input_file=input_file,
        output_path=output_path,
        domain=domainToExport,
        sample_size=exportSize,
        sample_output_folderpath=EXPORTS_FOLDER_PATH,
        exportUniquePredicates=False,
        setLogToInfo=False,
        stopSession=False
    )
    end_time = time.time()

    elapsed_time = end_time - start_time
    #t.join()
    print(f"===== Spark transformation done in {elapsed_time} sec =====\n")
    print("Number of rows : ", operationsLogs["nbRowsInit"])
    print("Number of duplicates : ", operationsLogs["nbDuplicates"])
    print("Number of rows after transformation : ", operationsLogs["nbRowsFinal"])
    print("Sampling : \nNumber of rows for the domain ", domainToExport, " : ", operationsLogs["nbFiltered"])
    print("Number of rows after sampling : ", operationsLogs["nbSampled"])
