import pytest
import os

@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    # Code to run before test

    yield # this is where the testing happens

    # Code to run after test
    # Remove the test database
    os.remove("test.db")
