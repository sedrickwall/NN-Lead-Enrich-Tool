# loaders/google_sheets.py
# Google Sheets (Published CSV) loader utilities for Streamlit

from __future__ import annotations

import pandas as pd
import streamlit as st


@st.cache_data(ttl=600)
def load_google_csv(url: str) -> pd.DataFrame:
    """
    Load a published Google Sheets CSV URL into a DataFrame.

    Notes:
    - This expects a URL like:
      https://docs.google.com/spreadsheets/d/e/.../pub?gid=...&single=true&output=csv
    - Cached for 10 minutes by default.
    """
    if not url or not isinstance(url, str):
        raise ValueError("Google CSV url must be a non-empty string.")

    # pandas will raise if it can't fetch/parse
    df = pd.read_csv(url)

    # Defensive cleanup: drop totally empty columns created by export quirks
    df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed")]

    return df


def clear_cache() -> None:
    """
    Clear Streamlit cache (useful for a "Refresh library data" button).
    """
    st.cache_data.clear()
