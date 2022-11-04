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
    # schema qualified table names with same rules
    "schema.all_alpha",
    "schema.ALL_CAPS_ALPHA",
    "schema_123.þis_is_nøn_latîn_ælpha",
    "schema.this_is_nøn_latîn_ælpha_except_first_character",
    "schema._starts_with_underscore",
    "schema.has_numbers_123",
    "schema.has_$",
    # rules also apply to schema names
    "all_alpha.table",
    "ALL_CAPS_ALPHA.table",
    "þis_is_nøn_latîn_ælpha.table",
    "this_is_nøn_latîn_ælpha_except_first_character.table",
    "_starts_with_underscore.table",
    "has_numbers_123.table",
    "has_$.table",
])
def test_validate_identifier_good(good_identifier):
    validate_identifier(good_identifier)


@pytest.mark.parametrize('bad_identifier', [
    "Robert'); DROP TABLE students; --",  # SQL injection attempt: https://xkcd.com/327/
    "",  # empty
    "123_starts_number",
    "$_starts_dollar",
    ";",
    "()",
    "schema.123_starts_number",
    "schema.$_starts_dollar",
    "123_starts_number.table",
    "$_starts_dollar.table",
    ";",
    "()",
])
def test_validate_identifier_bad(bad_identifier):
    with pytest.raises(ETLHelperBadIdentifierError):
        validate_identifier(bad_identifier)
