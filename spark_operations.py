import re
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, regexp_replace, regexp_extract, col
from pyspark.sql.types import StringType, StructType, StructField
import glob
import pandas as pd
import time
import os

os.environ['PYSPARK_PYTHON'] = 'C:/Users/blremi/birdlink/MEP/sandbox/workspace/v_env/Scripts/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/blremi/birdlink/MEP/sandbox/workspace/v_env/Scripts/python.exe'


class SparkOperations:
    def __init__(self, app_name, RDF_DATA_PATH, PREDICATES_TEMPLATE_PATH):
        self.sparkSession = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()

        self.context = self.sparkSession.sparkContext

        self.RDF_DATA_PATH = RDF_DATA_PATH
        self.PREDICATES_TEMPLATE_PATH = PREDICATES_TEMPLATE_PATH
        self.UNIQUE_PREDICATES_FILEPATH = self.RDF_DATA_PATH + "sparkedData/exploResults/unique_predicates"
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

    #@staticmethod
    def merge_sparked_data(self, folder, merged_filename, delim):
        # Récupérer la liste de tous les fichiers CSV dans le dossier
        files = glob.glob(folder + "/*.csv")

        print(folder + "/*.csv")
        df_list = [pd.read_csv(file, sep=delim, header=None) for file in files]

        merged_df = pd.concat(df_list, ignore_index=True)

        print(merged_df[0:10])

        #sorted_df = merged_df.sort_values(by=merged_df.columns[0])

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
        with open(self.PREDICATES_TEMPLATE_PATH, "r") as f:
            predicates_json = json.load(f)

        desired_predicates = extract_recursive(predicates_json[desired_domain], prefix=desired_domain)

        return desired_predicates
    def RDF_transform_and_sample_by_domain(self, input_file, output_path, domain, sample_size, sample_output_folderpath, exportUniquePredicates, setLogToInfo=False, stopSession=True):
        """
                Perform the data transformation of the given rdf-triples.csv file.
                This function only accept RDF formated data.
                After the transformation is performed, a sample from the given domain will
                be wrotte to a csv file.
                :param input_file: RDF formated .csv file on which we want to perform the transformation.
                :param output_path: Path where the sliced data from Spark transformations will be dumped.
                :param domain: Domain of the predicates that we want to sample.
                :param sample_size: Size of the sample, between 0 and 1.
                :param sample_output_folderpath: Folder where the samples of tripples will be dumped
                :param setLogToInfo: Set the Spark session's log level to "INFO". False by default.
                :param stopSession: Automaticaly stoping the Spark session when the function is done.
        """
        if setLogToInfo: self.sparkSession.sparkContext.setLogLevel("INFO")

        # Reading RDF-en-fr (230M)
        df = self.sparkSession.read \
            .option("delimiter", "\t") \
            .option("header", "false") \
            .option("inferSchema", "true") \
            .csv(input_file)

        # Apply the UDF to the Subject / Predicate columns + other columns transformations
        df = df.withColumn("_c0", self.transform_subject_udf(df["_c0"])) \
            .withColumn("_c1", self.transform_predicate_udf(df["_c1"])) \
            .withColumn("_c2", regexp_replace("_c2", r'^\\"(.*)\\"$', r'$1')) \
            .withColumn("_c2", regexp_extract("_c2", r'^(.*?)@', 1))

        # Getting rid of duplicates
        initial_count = df.count()
        df_no_duplicates = df.dropDuplicates()
        final_count = df_no_duplicates.count()
        duplicates_count = initial_count - final_count

        # Searching for samples based on the desired domain :
        desired_predicates = self.get_predicates_by_domain(desired_domain=domain)

        filtered_df = df.filter(df["_c1"].isin(desired_predicates))
        sample_df = filtered_df.sample(withReplacement=False, fraction=sample_size)

        filtered_count = filtered_df.count()
        sampled_count = sample_df.count()

        # Filtered and sampled file by predicates wrote to the output
        sample_df.write \
            .option("delimiter", "|") \
            .option("header", "false") \
            .mode("overwrite") \
            .csv(sample_output_folderpath + domain + "_triples")

        # Transformed file wrote to the output
        df.write \
            .option("delimiter", "|") \
            .option("header", "false") \
            .mode("overwrite") \
            .csv(output_path)

        # Getting sorted unique predicates
        if exportUniquePredicates :
            unique_predicates_file = self.UNIQUE_PREDICATES_FILEPATH
            unique_predicates = df.select("_c1").distinct()

            unique_predicates.write \
                .option("delimiter", "|") \
                .option("header", "false") \
                .mode("overwrite") \
                .csv(unique_predicates_file)

        # Arrêtez la session Spark
        if stopSession: self.sparkSession.stop()

        logs = {
            "nbRowsInit": initial_count,
            "nbRowsFinal": final_count,
            "nbDuplicates": duplicates_count,
            "nbFiltered": filtered_count,
            "nbSampled": sampled_count
        }

        return logs
