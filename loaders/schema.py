# loaders/schema.py
# Lightweight schema validation helpers for DataFrames

from __future__ import annotations

from typing import Iterable, List, Optional

import pandas as pd
import streamlit as st


def validate_columns(
    df: pd.DataFrame,
    required_cols: Iterable[str],
    name: str = "dataframe",
    *,
    stop_app: bool = True,
) -> List[str]:
    """
    Validate that required columns exist in df.
    Returns a list of missing columns.
    If stop_app=True, it will display a Streamlit error and st.stop() if missing.
    """
    required = [c for c in required_cols if c] if required_cols else []
    missing = [c for c in required if c not in df.columns]

    if missing and stop_app:
        st.error(f"❌ `{name}` is missing required columns: {missing}")
        st.stop()

    return missing


def coerce_str_columns(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    """
    Ensure the specified columns exist and are coerced to string (safe for IDs/domains).
    Missing cols are ignored.
    """
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].astype("string")
    return out


def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops export-artifact columns like 'Unnamed: 0'
    """
    return df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed")]
