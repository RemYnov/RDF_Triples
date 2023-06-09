import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from logs_management import Logger

def test_increment_and_get_counter():
    testLogger = Logger()
    delta = 300
    for i in range(0,delta):
        testLogger.custom_counter(counter="count1", increment=1)
        testLogger.custom_counter("count2", -2)
        testLogger.custom_counter("count3", 10)

    assert testLogger.get_counter("count1") == delta*1
    assert testLogger.get_counter("count2") == delta*(-2)
    assert testLogger.get_counter("count3") == delta*10
