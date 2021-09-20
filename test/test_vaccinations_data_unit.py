import pytest

from dags.utils import get_vaccination_data


def test_get_vaccination_data():
    excepted_row = 10
    file_path = "sample_vaccinations_data.csv"
    output_filename = "test.csv"
    assert excepted_row == get_vaccination_data(file_path, output_filename)
