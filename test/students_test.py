import pytest
import allure
from .conftest import attach_table


@pytest.fixture(scope='module', autouse=True)
@allure.title("Create students table")
def students_table(sql_context):
    students = sql_context.createDataFrame(
        [
            ('Vlad Dracula', 591),
            ('Harry Potter', 11),
            ('Dipper Pines', 21),
            ('Dipper Pines', 21),
        ],
        ['name', 'age'],
    )
    students.createOrReplaceTempView("students")
    attach_table(students, "students")
    return students


@allure.epic("Students")
@allure.story("Age")
@allure.title("There are adults only")
def test_babies(sql_context):
    result = sql_context.sql("SELECT name, age FROM students WHERE age < 10")
    babies_count = result.count()
    assert babies_count == 0


@allure.epic("Students")
@allure.story("Age")
@allure.title("There are no evil")
def test_vampires(sql_context):
    result = sql_context.sql("SELECT name, age FROM students WHERE age > 150")
    vampires = result.count()
    assert vampires == 0, "Expect no evil here but found {count} dark person(s)".format(count=vampires)


@allure.epic("Students")
@allure.story("Content")
@allure.title("Data is correct")
def test_duplicates(students_table):
    expected = students_table.count()
    result = students_table.distinct().count()
    assert result == expected,\
        "It's magic. Table size: {expected} but there are {result} student(s)".format(expected=expected, result=result)
