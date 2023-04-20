import re
import json
from logs_management import Logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, regexp_replace, regexp_extract, col
from pyspark.sql.types import StringType
import glob
import pandas as pd
import os
from urllib import parse
from config import PREDICATES_TEMPLATE_PATH

os.environ['PYSPARK_PYTHON'] = 'C:/Users/blremi/birdlink/MEP/sandbox/workspace/v_env/Scripts/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/blremi/birdlink/MEP/sandbox/workspace/v_env/Scripts/python.exe'


class SparkOperations:
    """
    Every spark operations will be managed and monitored
    from this class.
    """
    def __init__(self, app_name, RDF_DATA_PATH):
        # Init loggers
        self.sparkLoger = Logger(prefix="- spark -", defaultCustomLogs="fancy")
        self.sparkWarningLoger = Logger(prefix="- spark -", defaultCustomLogs="warning")
        self.sparkErrorLogger = Logger(prefix="- spark error -", defaultCustomLogs="critical")

        self.SPARK_LOCAL_DIR = parse.urljoin('file', parse.quote("sparkWorkspace"))
        self.SPARK_LOGS_DIR = parse.urljoin(self.SPARK_LOCAL_DIR, parse.quote("eventLogs"))
        msg1 = "Spark working on path : " + self.SPARK_LOCAL_DIR
        msg2 = "Spark logs stored at : " + self.SPARK_LOGS_DIR
        self.sparkWarningLoger.log(msg1)
        self.sparkWarningLoger.log(msg2)

        self.sparkSession = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .config("spark.executor.memoryOverHead", "1g") \
            .config("spark.local.dir", self.SPARK_LOCAL_DIR) \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", self.SPARK_LOCAL_DIR + "/eventLogs") \
            .getOrCreate()

        self.context = self.sparkSession.sparkContext

        self.RDF_DATA_PATH = RDF_DATA_PATH
        self.UNIQUE_PREDICATES_FILEPATH = self.RDF_DATA_PATH + "sparkedData/exploResults/unique_predicates"
        self.MATCHING_TRIPLES_PATH = self.RDF_DATA_PATH + "sparkedData/exploResults/matchingTriples"

        ### Registering the UDF ###
        # Needed when specifc calculations (without native pySpark function) need to be applyied to the data
        self.transform_subject_udf = udf(self.transform_subject, StringType())
        self.transform_predicate_udf = udf(self.transform_predicate, StringType())

    @staticmethod
    def transform_subject(s):
        """
                Apply transformation on the 'Subject' column.
                We isolate the Subject by getting rid of the url prefix and deleting the
                last char '>'.
                :param s: Element from the 'Subject' column.
        """
        prefix = "<http://rdf.freebase.com/ns/"
        if s.startswith(prefix):
            s = s[len(prefix):]
        if s[-1] == '>':
            s = s[:-1]
        return s
    @staticmethod
    def transform_predicate(s):
        """
                Apply transformation on the 'Predicate' column.
                We isolate the Predicate and translate some old syntax
                to new one.
                :param s: Element from the 'Predicate' column.
        """
        stripped_predicate = re.sub(r'^<.*\/(.*?)>$', r'\1', s)
        if stripped_predicate == "rdf-schema#label":
            stripped_predicate = "type.object.name"
        # to test:
        elif stripped_predicate == "rdf-syntax-ns#type":
            stripped_predicate = "type.object.type"
        return stripped_predicate
    def merge_sparked_data(self, folder, merged_filename, delim):
        # Récupérer la liste de tous les fichiers CSV dans le dossier
        files = glob.glob(folder + "/*.csv")

        print(folder + "/*.csv")
        df_list = [pd.read_csv(file, sep=delim, header=None) for file in files]

        merged_df = pd.concat(df_list, ignore_index=True)

        print(merged_df[0:10])

        # sorted_df = merged_df.sort_values(by=merged_df.columns[0])

        merged_df.to_csv(folder + merged_filename, index=False, sep=delim)
    def get_predicates_by_domain(self, desired_domain):
        """
                Return every unique predicates that match the given
                domain.
                Used when we want to get tripples data on a specific domains
                :param desired_domain: name of the domain we want to sample
        """
        # Retrieve each unique predicates based on the given predicates template
        def extract_recursive(json_dict, prefix='', predicates=None):
            if predicates is None:
                predicates = []

            for key, value in json_dict.items():
                current_prefix = f'{prefix}.{key}' if prefix else key
                if value:
                    extract_recursive(value, current_prefix, predicates)
                else:
                    predicates.append(current_prefix)

            return predicates

        ### Loading the predicate structure ###
        with open(PREDICATES_TEMPLATE_PATH, "r") as f:
            predicates_json = json.load(f)

        desired_predicates = extract_recursive(predicates_json[desired_domain], prefix=desired_domain)

        return desired_predicates
    def RDF_transform_and_sample_by_domain(
            self, input_file, output_path, exportConfig, performCounts, setLogToInfo=False, stopSession=True
    ):
        """
                Perform the data transformation of the given rdf-triples.csv file.
                This function only accept RDF formated data.
                After the transformation is performed, a sample from the given domain will
                be wrotte to a csv file.
                :param input_file: RDF formated .csv file on which we want to perform the transformation.
                :param output_path: Path where the sliced data from Spark transformations will be dumped.
                :param exportConfig: Dict that defines the sampling process (enable or not)
                    :param domain: Domain of the predicates that we want to sample.
                    :param sample_size: Size of the sample, between 0 and 1.
                    :param sample_output_folderpath: Folder where the samples of tripples will be dumped
                :param setLogToInfo: Set the Spark session's log level to "INFO". False by default.
                :param stopSession: Automaticaly stoping the Spark session when the function is done.
        """
        if setLogToInfo: self.sparkSession.sparkContext.setLogLevel("INFO")

        logs = {
            "nbRowsInit": 0,
            "nbRowsFinal": 0,
            "nbDuplicates": 0,
            "nbFiltered": 0,
            "nbSampled": 0
        }

        # Reading RDF-en-fr (230M)
        self.sparkLoger.start_timer("reading")
        df = self.sparkSession.read \
            .option("delimiter", "\t") \
            .option("header", "false") \
            .option("inferSchema", "true") \
            .csv(input_file)
        self.sparkLoger.stop_timer("reading")

        self.sparkLoger.start_timer("transformation")
        # Apply the UDF to the Subject / Predicate columns + other columns transformations
        df = df.withColumn("_c0", self.transform_subject_udf(df["_c0"])) \
            .withColumn("_c1", self.transform_predicate_udf(df["_c1"])) \
            .withColumn("_c2", regexp_replace("_c2", r'^\\"(.*)\\"$', r'$1')) \
            .withColumn("_c2", regexp_extract("_c2", r'^(.*?)@', 1))
        self.sparkLoger.stop_timer("transformation")

        # Getting rid of duplicates
        """
        self.sparkLoger.start_timer("processing duplicates")
        df_no_duplicates = df.dropDuplicates()

        if performCounts:
            initial_count = df.count()
            final_count = df_no_duplicates.count()
            duplicates_count = initial_count - final_count

        self.sparkLoger.stop_timer("processing duplicates")
        """

        if exportConfig["exportSampleEnabled"]:
            timer = "Export ", exportConfig["domainToExport"], " samples"
            self.sparkLoger.start_timer(timer)
            # Searching for samples based on the desired domain :
            desired_predicates = self.get_predicates_by_domain(desired_domain=exportConfig["domainToExport"])

            filtered_df = df.filter(df["_c1"].isin(desired_predicates))
            sample_df = filtered_df.sample(withReplacement=False, fraction=exportConfig["exportSize"])

            filtered_count = filtered_df.count()
            sampled_count = sample_df.count()

            # Filtered and sampled file by predicates wrote to the output
            sample_df.write \
                .option("delimiter", "|") \
                .option("header", "false") \
                .mode("overwrite") \
                .csv(exportConfig["sample_output_folderpath"] + exportConfig["domainToExport"] + "_triples")

            logs["nbFiltered"] = filtered_count
            logs["nbSampled"] = sampled_count
            self.sparkLoger.stop_timer(timer)
        else:
            logs["nbFiltered"] = 0
            logs["nbSampled"] = 0

        if exportConfig["exportMatchingTriples"] :
            self.sparkLoger.start_timer("looking for matching triples")

            # Searching for samples based on the desired domain :
            desired_predicates = self.get_predicates_by_domain(desired_domain=exportConfig["domainToExport"])
            filtered_df_by_domain = df.filter(df["_c1"].isin(desired_predicates))

            # Df with only subject / object
            subjects_df = df.select("_c0").distinct()
            objects_df = df.select("_c2").distinct()
            joined_df = subjects_df.crossJoin(objects_df)
            # Keeping only records where the subject exists in the object column
            filtered_df = joined_df.filter(col("_c2").contains(col("_c0")))

            matchingTriples_df = filtered_df_by_domain.join(filtered_df, on=["_c0", "_c2"], how="inner")
            self.sparkLoger.stop_timer("looking for matching triples")

            self.sparkLoger.start_timer("writting matching triples")
            filtered_df_by_domain.write \
                .option("delimiter", "|") \
                .option("header", "false") \
                .mode("overwrite") \
                .csv(self.MATCHING_TRIPLES_PATH)
            self.sparkLoger.stop_timer("writting matching triples")

        if exportConfig["exportFullData"]:
            # Transformed file wrote to the output
            self.sparkLoger.start_timer("Writting transformed file")
            df.write \
                .option("delimiter", "|") \
                .option("header", "false") \
                .mode("overwrite") \
                .csv(output_path)
            self.sparkLoger.stop_timer("Writting transformed file")

        # Getting sorted unique predicates
        if exportConfig["exportUniquePredicates"]:
            self.sparkLoger.start_timer("predicates export")
            unique_predicates_file = self.UNIQUE_PREDICATES_FILEPATH
            unique_predicates = df.select("_c1").distinct()

            unique_predicates.write \
                .option("delimiter", "|") \
                .option("header", "false") \
                .mode("overwrite") \
                .csv(unique_predicates_file)
            self.sparkLoger.stop_timer("predicates export")

        # Arrêtez la session Spark
        if stopSession:
            self.sparkSession.stop()

        logs["nbRowsInit"] = 0#initial_count
        logs["nbRowsFinal"] = 0#final_count
        logs["nbDuplicates"] = 0#duplicates_count

        return logs
