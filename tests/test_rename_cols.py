"""
Tests for Standardize.rename_cols duplicate-column handling.

Run from the repo root with either:
    python -m pytest tests/
    python tests/test_rename_cols.py
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline.components.standardize import Standardize  # noqa: E402

AFFECTED = "per_outage_customers_affected"


def make_std():
    std = Standardize(name="test", state="sc", date="2026-11-09")
    # Two raw columns map to customers affected, as in the SC/TX mappings before the % out fix
    std.col_map = {
        "county": "county",
        "cust_a": AFFECTED,
        "n_out": AFFECTED,
        "cust_s": "customers_served",
        "timestamp": "timestamp",
    }
    return std


def frame(cust_a, n_out):
    n = len(cust_a)
    return pd.DataFrame({
        "county": ["anderson"] * n,
        "cust_a": cust_a,
        "n_out": n_out,
        "cust_s": [100] * n,
        "timestamp": [f"11-09-2026 0{i}:00:00" for i in range(n)],
    })


def test_rows_equal_duplicate_columns_keeps_each_rows_value():
    # 2 rows, 2 duplicate columns: the case that used to repeat the first row's value
    out = make_std().rename_cols(frame([2, 10], [1, 1]))
    assert list(out[AFFECTED]) == [2, 10]


def test_three_rows_three_duplicate_columns():
    std = make_std()
    std.col_map["customersaffected"] = AFFECTED
    df = frame([2, 10, 10], [1, 1, 1])
    df["customersaffected"] = [9, 9, 9]
    out = std.rename_cols(df)
    assert list(out[AFFECTED]) == [2, 10, 10]


def test_falls_back_to_next_column_when_first_is_empty():
    out = make_std().rename_cols(frame([np.nan, "", " ", 4], [7, 8, 9, 1]))
    assert list(out[AFFECTED]) == [7, 8, 9, 4]


def test_result_has_one_column_per_name_in_original_order():
    out = make_std().rename_cols(frame([2, 10], [1, 1]))
    assert not out.columns.duplicated().any()
    assert list(out.columns) == ["county", AFFECTED, "customers_served", "timestamp"]


def test_no_duplicates_is_unchanged():
    std = make_std()
    del std.col_map["n_out"]
    df = frame([2, 10, 3], [1, 1, 1])
    out = std.rename_cols(df)
    assert list(out[AFFECTED]) == [2, 10, 3]


def test_empty_frame():
    out = make_std().rename_cols(frame([], []))
    assert out.empty
    assert list(out.columns) == ["county", AFFECTED, "customers_served", "timestamp"]


if __name__ == "__main__":
    tests = [v for k, v in dict(globals()).items() if k.startswith("test_")]
    for t in tests:
        t()
        print(f"passed: {t.__name__}")
    print(f"{len(tests)} tests passed")
