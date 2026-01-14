# loaders/normalize.py
# Domain/email normalization + simple company name cleaning utilities

from __future__ import annotations

import re
from typing import Optional

import pandas as pd


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def extract_email_domain(email: object) -> Optional[str]:
    """
    Returns the domain part of an email, or None if missing/invalid.
    """
    if email is None or (isinstance(email, float) and pd.isna(email)):
        return None

    s = str(email).strip().lower()
    if "@" not in s:
        return None

    # Use a mild validity check (optional)
    # If it doesn't match, still try to parse domain safely.
    parts = s.split("@")
    if len(parts) < 2:
        return None

    domain = parts[-1].strip()
    return domain or None


def normalize_domain(domain: object, collapse_subdomains: bool = True) -> Optional[str]:
    """
    Normalize a domain or URL-ish string into a comparable domain string.

    - lowercase
    - strips http/https, www, paths, trailing slashes
    - optionally collapses subdomains (naive: last two labels)

    NOTE: This naive collapse may not be correct for multi-level TLDs like .co.uk.
    If you need that later, switch to tldextract.
    """
    if domain is None or (isinstance(domain, float) and pd.isna(domain)):
        return None

    d = str(domain).strip().lower()
    if not d:
        return None

    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)

    # Remove path/query fragments
    d = d.split("/")[0].strip()
    d = d.split("?")[0].strip()
    d = d.split("#")[0].strip()

    if not d:
        return None

    if collapse_subdomains and d.count(".") >= 2:
        parts = d.split(".")
        d = ".".join(parts[-2:])

    return d or None


def normalize_website_to_domain(website: object, collapse_subdomains: bool = True) -> Optional[str]:
    """
    Converts a website field (which might be a full URL) to a normalized domain.
    """
    return normalize_domain(website, collapse_subdomains=collapse_subdomains)


_SUFFIXES_RE = re.compile(
    r"\b(inc|inc\.|llc|l\.l\.c\.|ltd|ltd\.|limited|corp|corp\.|corporation|co|co\.|company|gmbh|s\.a\.|sa|sarl)\b",
    re.IGNORECASE,
)


def clean_company_name(name: object) -> str:
    """
    Basic company name cleanup for potential future fuzzy matching:
    - lowercase
    - remove punctuation
    - remove common suffixes (Inc, LLC, etc.)
    - normalize whitespace
    """
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""

    s = str(name).strip().lower()
    if not s:
        return ""

    s = _SUFFIXES_RE.sub("", s)
    s = re.sub(r"[^\w\s]", " ", s)  # replace punctuation with space
    s = re.sub(r"\s+", " ", s).strip()

    return s
