import pytest

from lakehouse.nhs import clean_nhs_number, is_valid_nhs_number, nhs_check_digit


def test_known_valid_number():
    assert is_valid_nhs_number("9434765919")


def test_spaces_are_ignored():
    assert is_valid_nhs_number("943 476 5919")
    assert clean_nhs_number(" 943 476 5919 ") == "9434765919"


@pytest.mark.parametrize("value", [
    "9434765918",   # wrong check digit
    "943476591",    # nine digits
    "94347659190",  # eleven digits
    "94347659A9",   # not all digits
    "",
    None,
])
def test_invalid_numbers(value):
    assert not is_valid_nhs_number(value)


def test_check_digit_of_eleven_becomes_zero():
    # Find nine digits whose weighted total is a multiple of 11: the check digit must be 0
    for n in range(999_000_000, 999_001_000):
        first_nine = str(n)
        total = sum(int(d) * w for d, w in zip(first_nine, range(10, 1, -1)))
        if total % 11 == 0:
            assert nhs_check_digit(first_nine) == 0
            assert is_valid_nhs_number(first_nine + "0")
            return
    pytest.fail("no example found")


def test_check_digit_of_ten_is_invalid():
    for n in range(999_000_000, 999_001_000):
        first_nine = str(n)
        total = sum(int(d) * w for d, w in zip(first_nine, range(10, 1, -1)))
        if 11 - (total % 11) == 10:
            assert nhs_check_digit(first_nine) is None
            assert not any(is_valid_nhs_number(first_nine + str(d)) for d in range(10))
            return
    pytest.fail("no example found")


def test_check_digit_needs_nine_digits():
    with pytest.raises(ValueError):
        nhs_check_digit("12345")
