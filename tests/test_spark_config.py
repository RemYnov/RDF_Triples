import re
from pyspark.sql import SparkSession
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

#sys.path.append('C:/Users/blremi/birdlink/MEP/sandbox/workspace')

from spark_config import get_spark_ui_url, get_spark_info
def test_get_spark_ui_url():
    url_pattern = r'^http://.*:\d{4}$'
    sparkTestSession = SparkSession.builder \
        .appName("Spark UI test") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    url = get_spark_ui_url(sparkTestSession)
    sparkTestSession.stop()
    assert bool(re.match(url_pattern, url))

def test_get_spark_context():
    sparkVersion, masterUrl = get_spark_info()
    assert sparkVersion == "3.3.2"
    assert len(masterUrl) >= 1