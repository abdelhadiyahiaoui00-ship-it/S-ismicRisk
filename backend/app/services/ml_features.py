from __future__ import annotations

import numpy as np
import pandas as pd

from app.services.ml_preprocessing import ADEQUATE_RATES


def build_feature_matrix(portfolio: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    df = pd.DataFrame(index=portfolio.index)

    df["zone_sismique"] = portfolio["zone_sismique"].fillna("UNKNOWN").astype(str)
    df["wilaya_code"] = portfolio["wilaya_code"].fillna("UNKNOWN").astype(str)
    df["commune_name"] = portfolio["commune_name"].fillna("UNKNOWN").astype(str)
    df["type_risque"] = portfolio["type_risque"].fillna("UNKNOWN").astype(str)
    df["construction_type"] = portfolio["construction_type"].fillna("INCONNU").astype(str)

    df["log_valeur_assuree"] = np.log1p(portfolio["VALEUR_ASSURÉE"].clip(lower=0))
    df["log_prime_nette"] = np.log1p(portfolio["PRIME_NETTE"].clip(lower=0))
    df["prime_rate"] = portfolio["prime_rate"].clip(lower=0, upper=0.05).fillna(0.0)
    df["zone_num"] = portfolio["zone_num"].fillna(-1).astype(int)

    adequate_rates = portfolio["zone_sismique"].map(ADEQUATE_RATES).replace(0, np.nan)
    df["premium_adequacy_ratio"] = (
        (portfolio["prime_rate"] / adequate_rates)
        .replace([np.inf, -np.inf], np.nan)
        .clip(lower=0, upper=5)
        .fillna(0.0)
    )

    duration = (
        pd.to_datetime(portfolio["DATE_EXPIRATION"], errors="coerce")
        - pd.to_datetime(portfolio["DATE_EFFET"], errors="coerce")
    ).dt.days
    df["duration_days"] = duration.clip(lower=0, upper=730).fillna(365).astype(int)
    df["year"] = portfolio["year"].fillna(2025).astype(int)

    cat_features = [
        "zone_sismique",
        "wilaya_code",
        "commune_name",
        "type_risque",
        "construction_type",
    ]
    return df, cat_features
