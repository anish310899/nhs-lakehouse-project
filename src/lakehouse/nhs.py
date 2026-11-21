"""NHS number helpers (pure Python, no Spark needed).

An NHS number is 10 digits. The 10th digit is a check digit calculated with
the Modulus 11 algorithm:

1. Multiply each of the first nine digits by a weight from 10 down to 2.
2. Add the results together.
3. Divide by 11 and take the remainder.
4. Subtract the remainder from 11.
   - A result of 11 means the check digit is 0.
   - A result of 10 means the number is invalid.

This project only ever generates numbers in the 999 range, which the NHS
reserves for testing, so the synthetic data can never match a real patient.
"""

from __future__ import annotations

import re

WEIGHTS = range(10, 1, -1)  # 10, 9, 8, 7, 6, 5, 4, 3, 2


def clean_nhs_number(value: str | None) -> str | None:
    """Remove spaces so '943 476 5919' becomes '9434765919'."""
    if value is None:
        return None
    cleaned = re.sub(r"\s", "", str(value))
    return cleaned or None


def nhs_check_digit(first_nine: str) -> int | None:
    """Return the check digit for nine digits, or None if no valid digit exists."""
    if not re.fullmatch(r"\d{9}", first_nine):
        raise ValueError("first_nine must be exactly nine digits")
    total = sum(int(digit) * weight for digit, weight in zip(first_nine, WEIGHTS))
    check = 11 - (total % 11)
    if check == 11:
        return 0
    if check == 10:
        return None
    return check


def is_valid_nhs_number(value: str | None) -> bool:
    """True if the value is a 10-digit NHS number with a correct check digit."""
    cleaned = clean_nhs_number(value)
    if cleaned is None or not re.fullmatch(r"\d{10}", cleaned):
        return False
    expected = nhs_check_digit(cleaned[:9])
    return expected is not None and expected == int(cleaned[9])
