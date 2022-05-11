import pytest
import allure
from pyspark.sql.functions import max as spark_max
from .conftest import attach_table, attach_chart


@pytest.fixture(scope='module', autouse=True)
@allure.title("Create mountains table")
def mountains_table(sql_context):
    mountains = sql_context.createDataFrame(
        [
            ('Mount Elbrus', 'Earth', 5642),
            ('Mount Everest', 'Earth', 8850),
            ('Olympus Mons', 'Mars', 21900),
        ],
        ['name', 'planet', 'height'],
    )
    mountains.createOrReplaceTempView("mountains")
    attach_table(mountains, "mountains")
    return mountains


@allure.step("Find champion")
def find_champion(mountains):
    return mountains.agg(spark_max("height")).head()[0]


@allure.epic("Mountains")
@allure.story("Height")
@allure.title("We are still on Earth")
def test_earth(sql_context, current_planet):
    with allure.step("Select current planet mountains"):
        sql = 'SELECT * FROM mountains WHERE planet = "{planet}"'.format(planet=current_planet)
        mountains = sql_context.sql(sql)
        attach_table(mountains, "mountains")

    peak = find_champion(mountains)
    assert peak == 8850


@allure.epic("Mountains")
@allure.story("Height")
@allure.title("Mars is coolest")
def test_best(sql_context, mountains_table):
    attach_chart(sql_context, "height", "mountains")
    peak = find_champion(mountains_table)
    assert peak == 21900
