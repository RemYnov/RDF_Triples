import json
import os
import re
from urllib import parse

from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, regexp_replace, regexp_extract, col, explode
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, FloatType, ArrayType, StructType, StructField

from config import PREDICATES_TEMPLATE_PATH, RDF_DATA_PATH
from logs_management import Logger

os.environ['PYSPARK_PYTHON'] = 'C:/Users/blremi/birdlink/MEP/sandbox/workspace/v_env/Scripts/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/blremi/birdlink/MEP/sandbox/workspace/v_env/Scripts/python.exe'


class SparkOperations:
    """
    Every spark operations will be managed and monitored
    from this class.
    """
    def __init__(self, app_name, RDF_DATA_PATH, botLoggerEnabled):
        # Init loggers
        self.sparkLoger = Logger(prefix="- spark -", defaultCustomLogs="normal", botEnabled=botLoggerEnabled)

        self.SPARK_LOCAL_DIR = parse.urljoin('file', parse.quote("sparkWorkspace"))
        self.SPARK_LOGS_DIR = parse.urljoin(self.SPARK_LOCAL_DIR, parse.quote("eventLogs"))

        self.sparkSession = SparkSession.builder \
            .appName(app_name) \
            .config("spark.executorEnv.PYTHONHASHSEED", "0") \
            .config('spark.executor.heartbeatInterval', 10000) \
            .config("spark.executor.memory", "6g") \
            .config("spark.executor.memoryOverHead", "1g") \
            .config("spark.python.worker.memory", "2g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.driver.maxResultSize", "3g") \
            .config("spark.python.worker.reuse", "true") \
            .config("spark.default.parallelism", "200") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
            .config('spark.network.timeout', 10000) \
            .config("spark.core.connection.ack.wait.timeout", "3600") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", self.SPARK_LOCAL_DIR + "/eventLogs") \
            .config("spark.local.dir", self.SPARK_LOCAL_DIR) \
            .getOrCreate()

        self.context = self.sparkSession.sparkContext

        self.RDF_DATA_PATH = RDF_DATA_PATH
        self.UNIQUE_PREDICATES_FILEPATH = self.RDF_DATA_PATH + "sparkedData/exploResults/unique_predicates"
        self.MATCHING_TRIPLES_PATH = self.RDF_DATA_PATH + "sparkedData/exploResults/matchingTriples"

        ### Registering the UDF ###
        # Needed when specifc calculations (without native pySpark function) need to be applyied to the data
        self.transform_subject_udf = udf(self.transform_subject, StringType())
        self.transform_predicate_udf = udf(self.transform_predicate, StringType())
        self.transform_object_udf = udf(self.transform_object, ArrayType(StringType()))
        self.compute_similarity_udf = udf(self.compute_similarity_criteria, FloatType())
        self.min_len_sw = 50

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
        elif stripped_predicate == "rdf-syntax-ns#type":
            stripped_predicate = "type.object.type"
        return stripped_predicate

    @staticmethod
    def transform_object(tokens, object_init):
        min_len_sw = 50
        if len(object_init) >= min_len_sw:
            return [token for token in tokens if token not in StopWordsRemover().getStopWords()]
        else:
            return tokens

    @staticmethod
    def apply_NLP_pipeline(df):
        """
        NLP pipeline improving match efficience.
        Tokenisation + French and English stop words removal
        :param df: expected a cleaned dataframe with ['Subject', 'Predicate', 'Object']
        :return: a tokenized dataframe with ['Subject', 'Predicate', 'tokenizedObj', 'filtered_obj']
        """
        french_stopwords = StopWordsRemover.loadDefaultStopWords('french')
        english_stopwords = StopWordsRemover.loadDefaultStopWords('english')

        combined_stopwords = french_stopwords + english_stopwords

        # Pattern taking into account French syntax
        regex_tokenizer = RegexTokenizer(inputCol="Object", outputCol="tokenizedObj", pattern='[^\wÀ-ÖØ-öø-ÿ]+')
        stop_words_remover = StopWordsRemover(inputCol="tokenizedObj", outputCol="filtered_tokens",
                                              stopWords=combined_stopwords)

        nlp_pipeline = Pipeline(stages=[regex_tokenizer, stop_words_remover])

        nlp_model = nlp_pipeline.fit(df)
        tokenized_df = nlp_model.transform(df)

        return tokenized_df

    @staticmethod
    def compute_similarity_criteria(obj, matchedObj):
        """
        Compute a score between 0 and 1 using Jaccard's distance.
        Accepted hreshold set with the RDF_Graph_Model() object, initiated in
        main.py.
        :param obj: (NLP processed) The name of the Subject who mathed the Subject having for object 'matchedObj'
        :param matchedObj: (NLP processed) Object that match the name of the current Subject
        :return:
        """
        setObj = set(obj)
        setMatchedObj = set(matchedObj)

        intersection_size = len(setObj.intersection(setMatchedObj))
        union_size = len(setObj.union(setMatchedObj))

        if union_size == 0:
            return 0.0
        else:
            return float(intersection_size) / float(union_size)

    @staticmethod
    def get_predicates_by_domain(desired_domain):
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

    @staticmethod
    def get_unique_subject_names(df):
        subject_names = df.filter(col("Predicate").endswith("name")).select("Subject", "Predicate", "Object", "filtered_tokens")
        return subject_names.dropDuplicates()

    def RDF_transform_and_sample_by_domain(
            self, input_file, exportConfig,
            performCounts=False, setLogToInfo=False, stopSession=True, showSample=True
    ):
        """
                Perform the data transformation of the given rdf-triples.csv file.
                This function only accept RDF formated data.
                After the transformation is performed, a sample from the given domain will
                be wrotte to a csv file.
                :param input_file: RDF formated .csv file on which we want to perform the transformation.
                :param exportConfig: Dict that defines the sampling process (enable or not)
                :param setLogToInfo: Set the Spark session's log level to "INFO". False by default.
                :param stopSession: Automaticaly stoping the Spark session when the function is done.
                :param showSample: Display some records to the console at each step of the transformation
        """

        if setLogToInfo: self.sparkSession.sparkContext.setLogLevel("INFO")

        triples_schema = StructType([
            StructField("Subject", StringType(), nullable=False),
            StructField("Predicate", StringType(), nullable=False),
            StructField("Object", StringType(), nullable=False),
            StructField("Blank", StringType(), nullable=False)
        ])

        # Reading RDF-en-fr (230M)
        # .option("inferSchema", "true") \ => Detect schema
        self.sparkLoger.start_timer("reading")
        df = self.sparkSession.read \
            .option("delimiter", "\t") \
            .option("header", "false") \
            .schema(triples_schema) \
            .csv(input_file)
        df = (df.drop("Blank")).limit(50000)
        self.sparkLoger.stop_timer("reading")

        self.sparkLoger.start_timer("droping duplicates")
        df_light = df.dropDuplicates()
        self.sparkLoger.stop_timer("droping duplicates")

        if showSample:
            self.sparkLoger.log("RAW DF :")
            df_light.show(25, truncate=False)

        # Getting rid of duplicates
        if performCounts:
            self.sparkLoger.start_timer("perform counts")
            initial_count = df.count()
            final_count = df_light.count()
            duplicates_count = initial_count - final_count

            self.sparkLoger.custom_counter("initCount", initial_count)
            self.sparkLoger.custom_counter("finalCount", final_count)
            self.sparkLoger.custom_counter("duplicatesCount", duplicates_count)
            self.sparkLoger.stop_timer("perform counts")

        self.sparkLoger.start_timer("transformation")
        # Apply the UDF to the Subject / Predicate columns + other columns transformations
        cleaned_df = df_light.withColumn("Subject", self.transform_subject_udf(df_light["Subject"])) \
            .withColumn("Predicate", self.transform_predicate_udf(df_light["Predicate"])) \
            .withColumn("Object", regexp_replace("Object", r'^\\"(.*)\\"$', r'$1')) \
            .withColumn("Object", regexp_extract("Object", r'^(.*?)@', 1))
        self.sparkLoger.stop_timer("transformation")

        if showSample:
            self.sparkLoger.start_timer("PRINT CLEANED DF")
            cleaned_df.show(25, truncate=False)
            self.sparkLoger.stop_timer("PRINT CLEANED DF")

        # Will perform extract depending on the exportConfig param
        self.extract_sample(exportConfig, cleaned_df)

        self.sparkLoger.start_timer("NLP Pipeline")
        tokenized_df = self.apply_NLP_pipeline(cleaned_df)
        self.sparkLoger.stop_timer("NLP Pipeline")

        if showSample:
            self.sparkLoger.start_timer("PRINT TRANSFORMED DF")
            tokenized_df.show(50, truncate=False)
            self.sparkLoger.stop_timer("PRINT TRANSFORMED DF")

        if stopSession:
            self.sparkSession.stop()

        return self.sparkLoger.get_timer_counter(), tokenized_df


    def find_matching_triples(self, main_df, graph):
        """
                Joining the exploded objects from 'exploded_subject_df', which represents
                the name of each unique Triples Subject, with the 'exploded_main_df' which represents
                the exploded objects of the entire dataframe.
                :param main_df: the processed RDF triples dataframe, with cleaned data and tokenized object
                :return: dataframe having, for each unique subject of our main df, the matching triples
                based on the (tokenized) names of the unique subjects and the (tokenized) Objects of the main df
        """
        matchingLogger = Logger(prefix="- relations search -", defaultCustomLogs="warning", botEnabled=True)

        matchingLogger.start_timer("Explode objects")
        subject_names_df = self.get_unique_subject_names(df=main_df)
        # Unique subjects with their exploded tokenized name (cf NLP pipeline)
        # (== 1 row by token with the same subject key).
        # We take the col where the stop_words were applied : we don't want any match based
        # on stop_words.
        exploded_subject_df = subject_names_df.select(
            "Subject",
            col("Object").alias("SubjectNameFromObject"),
            col("filtered_tokens").alias("tokenizedSubName"),
            explode(col("filtered_tokens")).alias("exploded_tokens")
        )
        # Entire Triples data with the exploded objects
        # (only tokenized, stop words are still in those objects)
        exploded_main_df = main_df.select(
            "Subject",
            "Predicate",
            "Object",
            "tokenizedObj",
            explode(col("tokenizedObj")).alias("exploded_objects_tokens")
        )
        matchingLogger.stop_timer("Explode objects")

        matchingLogger.start_timer("matching triples")
        matched_triples = exploded_subject_df.alias("a")\
            .join(exploded_main_df.alias("b"), col("a.exploded_tokens") == col("b.exploded_objects_tokens"),
                  how="inner") \
            .select("a.Subject",
                    col("a.SubjectNameFromObject").alias("SubjectName"),
                    "a.tokenizedSubName",
                    col("b.Subject").alias("MatchedSubject"),
                    "b.Predicate",
                    col("b.tokenizedObj").alias("MatchedTokens"),
                    col("b.Object").alias("InitialMatchedObject")
                    )

        clean_matchs = matched_triples.dropDuplicates()

        #allMatchsCount = matched_triples.count()
        uniqueMatchsCount = clean_matchs.count()
        #duplicatedMatchs = allMatchsCount - uniqueMatchsCount

        #matchingLogger.custom_counter("nbMatchs", allMatchsCount)
        matchingLogger.custom_counter("nbUniqueMatchs", uniqueMatchsCount)
        #matchingLogger.custom_counter("nbDuplicatedMatchs", duplicatedMatchs)
        matchingLogger.stop_timer("matching triples")

        matchingLogger.start_timer("Keeping only similar match")
        triples_to_modelise = clean_matchs.withColumn(
            "similarity_rate",
            self.compute_similarity_udf(col("tokenizedSubName"), col("MatchedTokens"))
        )

        similar_match_df = triples_to_modelise.filter(
            (col("similarity_rate") >= graph.min_similarity_rate) & (col("similarity_rate") <= graph.max_similarity_rate))
        nbSimilarMatch = similar_match_df.count()
        matchingLogger.custom_counter("nbSimilarMatch", nbSimilarMatch)
        matchingLogger.stop_timer("Keeping only similar match")

        matchingLogger.start_timer("Shuffle / Sample")
        similarMatch_shuffled = similar_match_df.withColumn('rand', F.rand()).orderBy('rand')
        related_subjects_random = similarMatch_shuffled.limit(300)
        related_subjects_random.show(truncate=False)
        matchingLogger.stop_timer("Shuffle / Sample")

        return triples_to_modelise, matchingLogger.get_timer_counter()

    def parquet_reading(self, parquet_dir, csv_file_path="null"):
        self.sparkLoger.start_timer("parquet to csv")
        parquet_df = self.sparkSession.read.parquet(parquet_dir)
        parquet_df.show(50, truncate=False)

        if csv_file_path != "null":
            parquet_df.write.csv(csv_file_path, mode="overwrite", header=True)

        self.sparkLoger.stop_timer("parquet to csv")

    def extract_sample(self, exportConfig, df):
        # Writting to csv a sample of triples from the given domain
        if exportConfig["exportSampleEnabled"]:
            timer = "Export " + exportConfig["domainToExport"] + " samples"
            self.sparkLoger.start_timer(timer)
            # Searching for samples based on the desired domain :
            desired_predicates = self.get_predicates_by_domain(desired_domain=exportConfig["domainToExport"])

            filtered_df = df.filter(df["Predicate"].isin(desired_predicates))
            sample_df = filtered_df.sample(withReplacement=False, fraction=exportConfig["exportSize"])

            filtered_count = filtered_df.count()
            sampled_count = sample_df.count()

            # Filtered and sampled file by predicates wrote to the output
            sample_df.write \
                .option("delimiter", "|") \
                .option("header", "false") \
                .mode("overwrite") \
                .csv(exportConfig["sample_output_folderpath"] + exportConfig["domainToExport"] + "_triples")

            self.sparkLoger.custom_counter("domainCount", filtered_count)
            self.sparkLoger.custom_counter("sample", sampled_count)
            self.sparkLoger.stop_timer(timer)

        # Writting to csv the matching triples
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
            matchingTriples_df.write \
                .option("delimiter", "|") \
                .option("header", "false") \
                .mode("overwrite") \
                .csv(self.MATCHING_TRIPLES_PATH)
            self.sparkLoger.stop_timer("writting matching triples")

        # Writting to csv the entire dataset
        if exportConfig["exportFullData"]:
            # Transformed file wrote to the output
            self.sparkLoger.start_timer("Writting transformed file")
            df.write \
                .option("delimiter", "|") \
                .option("header", "false") \
                .mode("overwrite") \
                .csv(exportConfig["exportFullPath"])
            self.sparkLoger.stop_timer("Writting transformed file")

        # Wrtting to csv sorted unique predicates
        if exportConfig["exportUniquePredicates"]:
            self.sparkLoger.start_timer("predicates export")
            unique_predicates_file = self.UNIQUE_PREDICATES_FILEPATH
            unique_predicates = df.select("Predicate").distinct()

            unique_predicates.write \
                .option("delimiter", "|") \
                .option("header", "false") \
                .mode("overwrite") \
                .csv(unique_predicates_file)
            self.sparkLoger.stop_timer("predicates export")