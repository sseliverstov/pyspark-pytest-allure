import nox


@nox.session
def tests(session):
    session.run("pytest", "--capture=no", "--alluredir=./allure-results", "--clean-alluredir")


@nox.session
def allure(session):
    session.run("allure", "serve")
