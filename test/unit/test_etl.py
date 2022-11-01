"""Tests for etl functions"""
import pytest

from etlhelper.etl import validate_identifier
from etlhelper.exceptions import ETLHelperBadIdentifierError


@pytest.mark.parametrize('good_identifier', [
    "all_alpha",
    "ALL_CAPS_ALPHA",
    "þis_is_nøn_latîn_ælpha",
    "this_is_nøn_latîn_ælpha_except_first_character",
    "_starts_with_underscore",
    "has_numbers_123",
    "has_$",
])
def test_validate_identifier_good(good_identifier):
    validate_identifier(good_identifier)


@pytest.mark.parametrize('bad_identifier', [
    "Robert'); DROP TABLE students; --",  # SQL injection attempt: https://xkcd.com/327/
    "",  # empty
    "123_starts_number",
    "$_starts_dollar",
    ";",
    "()"
])
def test_validate_identifier_bad(bad_identifier):
    with pytest.raises(ETLHelperBadIdentifierError):
        validate_identifier(bad_identifier)
