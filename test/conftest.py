from pyspark.sql import SparkSession
import pytest
import allure
import io
import matplotlib.pyplot as plt


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


def attach_chart(sql_context, column, table):
    data = sql_context.sql("select {column} from {table}".format(column=column, table=table))
    plt.figure()
    data.toPandas().plot(kind="bar")
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    allure.attach(buf.read(), table, allure.attachment_type.PNG)
