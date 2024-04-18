from lib.Utils import get_spark_session
import pytest

@pytest.fixture(scope="session")
def spark():
    "create spark session"
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="function")
def expected_results(spark):
    "gives the expected results"
    results_schema = "state string, count integer"
    return spark.read \
        .format("csv") \
        .schema(results_schema) \
        .load("data/test_result/state_aggregate.csv")