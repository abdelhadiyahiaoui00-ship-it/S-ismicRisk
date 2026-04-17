from __future__ import annotations

import re

import numpy as np
import pandas as pd

ZONE_TO_NUM = {"0": 0, "I": 1, "IIa": 2, "IIb": 3, "III": 4}
ADEQUATE_RATES = {"0": 0.0005, "I": 0.0012, "IIa": 0.0028, "IIb": 0.0045, "III": 0.0075}

TYPE_UNKNOWN = "UNKNOWN"
TYPE_BIEN = "BIEN_IMMOBILIER"
TYPE_COMMERCIAL = "INSTALLATION_COMMERCIALE"
TYPE_INDUSTRIAL = "INSTALLATION_INDUSTRIELLE"

CONSTRUCTION_UNKNOWN = "INCONNU"
CONSTRUCTION_MASONRY = "MACONNERIE"
CONSTRUCTION_CONCRETE = "BETON_ARME"
CONSTRUCTION_STEEL = "STRUCTURE_METALLIQUE"

TYPE_TO_CONSTRUCTION = {
    TYPE_BIEN: CONSTRUCTION_MASONRY,
    TYPE_COMMERCIAL: CONSTRUCTION_CONCRETE,
    TYPE_INDUSTRIAL: CONSTRUCTION_STEEL,
    TYPE_UNKNOWN: CONSTRUCTION_UNKNOWN,
}


def _clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value).strip())


def normalize_zone(value: object) -> str:
    text = _clean_text(value).upper().replace(" ", "")
    if text in {"0", "I", "IIA", "IIB", "III"}:
        if text == "IIA":
            return "IIa"
        if text == "IIB":
            return "IIb"
        return text
    return "UNKNOWN"


def normalize_wilaya_code(value: object) -> str:
    text = _clean_text(value)
    if not text or text.lower() == "nan":
        return "UNKNOWN"
    match = re.search(r"(\d+)", text)
    if not match:
        return "UNKNOWN"
    return f"{int(match.group(1)):02d}"


def normalize_type_risque(value: object) -> str:
    text = _clean_text(value).lower()
    if not text:
        return TYPE_UNKNOWN
    if "industri" in text:
        return TYPE_INDUSTRIAL
    if "commercial" in text:
        return TYPE_COMMERCIAL
    if "bien immobilier" in text or "immobilier" in text:
        return TYPE_BIEN
    return TYPE_UNKNOWN


def normalize_construction_type(value: object, type_risque: object | None = None) -> str:
    text = _clean_text(value).lower()
    if text:
        if "metal" in text:
            return CONSTRUCTION_STEEL
        if "beton" in text:
            return CONSTRUCTION_CONCRETE
        if "ma" in text and "onner" in text:
            return CONSTRUCTION_MASONRY
    normalized_type = normalize_type_risque(type_risque)
    return TYPE_TO_CONSTRUCTION.get(normalized_type, CONSTRUCTION_UNKNOWN)


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _coerce_datetimes(series: pd.Series) -> pd.Series:
    raw = pd.Series(series, copy=False)
    text = raw.astype("string")
    iso_mask = text.str.fullmatch(r"\d{4}-\d{2}-\d{2}").fillna(False)

    parsed = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns]")
    if iso_mask.any():
        parsed.loc[iso_mask] = pd.to_datetime(raw.loc[iso_mask], errors="coerce", format="%Y-%m-%d")
    if (~iso_mask).any():
        parsed.loc[~iso_mask] = pd.to_datetime(raw.loc[~iso_mask], errors="coerce", dayfirst=True)
    return parsed


def prepare_portfolio_for_model(portfolio: pd.DataFrame, *, consolidate: bool = False) -> pd.DataFrame:
    del consolidate
    df = portfolio.copy()

    if "VALEUR_ASSUREE" in df.columns and "VALEUR_ASSURÉE" not in df.columns:
        df = df.rename(columns={"VALEUR_ASSUREE": "VALEUR_ASSURÉE"})
    if "TYPE" not in df.columns and "type_risque" in df.columns:
        df["TYPE"] = df["type_risque"]
    if "zone_sismique" not in df.columns:
        df["zone_sismique"] = "UNKNOWN"
    if "wilaya_code" not in df.columns:
        df["wilaya_code"] = "UNKNOWN"
    if "commune_name" not in df.columns:
        df["commune_name"] = "UNKNOWN"
    if "construction_type" not in df.columns:
        df["construction_type"] = pd.NA

    df["DATE_EFFET"] = _coerce_datetimes(df.get("DATE_EFFET", pd.Series(pd.NaT, index=df.index)))
    df["DATE_EXPIRATION"] = _coerce_datetimes(df.get("DATE_EXPIRATION", pd.Series(pd.NaT, index=df.index)))

    df["zone_sismique"] = df["zone_sismique"].map(normalize_zone)
    df["wilaya_code"] = df["wilaya_code"].map(normalize_wilaya_code)
    df["commune_name"] = df["commune_name"].map(lambda value: _clean_text(value).upper() or "UNKNOWN")
    df["type_risque"] = df["TYPE"].map(normalize_type_risque)
    df["construction_type"] = [
        normalize_construction_type(raw_construction, raw_type)
        for raw_construction, raw_type in zip(df["construction_type"], df["type_risque"])
    ]

    df["VALEUR_ASSURÉE"] = _coerce_numeric(df["VALEUR_ASSURÉE"])
    df["PRIME_NETTE"] = _coerce_numeric(df.get("PRIME_NETTE", 0.0))
    df["year"] = pd.to_numeric(df.get("year", pd.NA), errors="coerce")
    df["year"] = df["year"].fillna(df["DATE_EFFET"].dt.year).fillna(2025).astype(int)

    df["prime_rate"] = np.where(
        df["VALEUR_ASSURÉE"] > 0,
        df["PRIME_NETTE"] / df["VALEUR_ASSURÉE"],
        np.nan,
    )
    df["zone_num"] = df["zone_sismique"].map(ZONE_TO_NUM).fillna(-1).astype(int)
    adequate_rates = df["zone_sismique"].map(ADEQUATE_RATES).replace(0, np.nan)
    df["premium_adequacy_ratio"] = (
        (df["prime_rate"] / adequate_rates).replace([np.inf, -np.inf], np.nan).clip(lower=0, upper=5)
    )
    return df
