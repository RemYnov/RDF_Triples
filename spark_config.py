from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import time

# Contexte Spark
@staticmethod
def get_spark_context():

    conf = SparkConf().setAppName("Spark Version Check")
    sc = SparkContext(conf=conf)
    print("Spark version: ", sc.version)

    spark = SparkSession.builder \
        .appName("MyApp") \
        .getOrCreate()
    spark_master_url =  spark.conf.get("spark.master")
    print("Spark master URL:", spark_master_url)

    sc.stop()
    spark.stop()

def get_spark_ui_url(spark_session: SparkSession) -> str:
    # Récupération du nom d'hôte et du port du Spark UI
    ui_web_url = spark_session.sparkContext.uiWebUrl

    # Vérification si un proxy inverse est utilisé
    if spark_session.conf.get("spark.ui.reverseProxy", "false").lower() == "true":
        base_path = spark_session.conf.get("spark.ui.reverseProxyBase", "")
        ui_web_url = f"{ui_web_url.rstrip('/')}/{base_path.lstrip('/')}"

    return ui_web_url

def print_progress(sc, timeout):
    timeout_seconds = timeout * 60 * 60
    start_time = time.time()
    tracker = sc.statusTracker()
    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout_seconds:
            print("Timeout reached")
            break

        active_jobs = sc._jsc.sc().jobProgressListener().activeJobs()
        if not active_jobs:
            break

        for job_id in active_jobs:
            job_info = tracker.getJobInfo(job_id)
            if job_info is not None:
                num_stages = len(job_info.stageIds)
                completed_stages = 0
                for stage_id in job_info.stageIds:
                    stage_info = tracker.getStageInfo(stage_id)
                    if stage_info is not None and stage_info.status == "COMPLETE":
                        completed_stages += 1
                print(f"Job {job_id}: {completed_stages}/{num_stages} stages completed")

        time.sleep(5)  # Mettre à jour la progression toutes les 5 secondes

