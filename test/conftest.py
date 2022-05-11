from pyspark.sql import SparkSession
import pytest
import allure


@pytest.fixture(scope='session')
@allure.title("pySpark")
def sql_context():
    spark = SparkSession.builder.appName('Test').getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope='session')
def current_planet():
    return 'Earth'


def attach_table(table, name):
    csv = table.to_pandas_on_spark().to_csv()
    allure.attach(csv, name, allure.attachment_type.CSV)
