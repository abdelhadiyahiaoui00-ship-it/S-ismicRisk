from __future__ import annotations

import json
import pickle
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier

from app.core.config import settings
from app.schemas.ml import FeatureImportanceItem, PolicyScoreRequest
from app.services.ml_features import build_feature_matrix
from app.services.ml_preprocessing import ADEQUATE_RATES, prepare_portfolio_for_model


class MLService:
    def __init__(self) -> None:
        self.model: CatBoostClassifier | None = None
        self.metadata: dict[str, Any] | None = None
        self.training_metrics: dict[str, Any] | None = None
        self.feature_importance_cache: list[FeatureImportanceItem] | None = None
        self.tier_labels = {0: "LOW", 1: "MEDIUM", 2: "HIGH"}
        self.model_path = Path(__file__).resolve().parents[1] / "ml_models" / Path(settings.catboost_model_path).name
        self.metadata_path = Path(__file__).resolve().parents[1] / "ml_models" / "feature_metadata.pkl"
        self.metrics_path = Path(__file__).resolve().parents[1] / "ml_models" / "training_metrics.json"
        self.feature_importance_path = Path(__file__).resolve().parents[1] / "ml_models" / "feature_importance.csv"

    def load_models(self) -> None:
        if self.model is not None:
            return

        model = CatBoostClassifier()
        model.load_model(str(self.model_path))
        self.model = model

        with self.metadata_path.open("rb") as file:
            self.metadata = pickle.load(file)

        if self.metrics_path.exists():
            self.training_metrics = json.loads(self.metrics_path.read_text(encoding="utf-8"))

        if self.feature_importance_path.exists():
            importance_df = pd.read_csv(self.feature_importance_path)
            self.feature_importance_cache = [
                FeatureImportanceItem(
                    name=str(row["feature"]),
                    importance=round(float(row["importance"]), 4),
                )
                for _, row in importance_df.sort_values("importance", ascending=False).iterrows()
            ]
        elif self.metadata:
            importances = self.model.get_feature_importance()
            names = self.metadata.get("feature_names", [])
            self.feature_importance_cache = [
                FeatureImportanceItem(name=name, importance=round(float(importance), 4))
                for name, importance in sorted(zip(names, importances), key=lambda item: -item[1])
            ]

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "model_loaded": self.model is not None,
            "model_path": str(self.model_path),
            "metadata_loaded": self.metadata is not None,
            "training_metrics": self.training_metrics,
        }

    def score_policy(self, policy_data: PolicyScoreRequest | dict[str, Any]) -> dict[str, Any]:
        self._ensure_loaded()
        payload = policy_data.model_dump() if isinstance(policy_data, PolicyScoreRequest) else dict(policy_data)

        started = perf_counter()
        prepared = self._prepare_policy_frame([payload])
        features, _ = build_feature_matrix(prepared)
        features = self._align_feature_order(features)

        probabilities = self.model.predict_proba(features)[0]
        class_idx = int(np.argmax(probabilities))
        risk_score = float(probabilities[2] * 100)
        normalized = prepared.iloc[0]

        return {
            "score": round(risk_score, 1),
            "tier": self.tier_labels[class_idx],
            "proba": {
                "LOW": round(float(probabilities[0]) * 100, 1),
                "MEDIUM": round(float(probabilities[1]) * 100, 1),
                "HIGH": round(float(probabilities[2]) * 100, 1),
            },
            "dominant_factor": self._get_dominant_factor(normalized),
            "normalized_inputs": {
                "zone_sismique": normalized["zone_sismique"],
                "wilaya_code": normalized["wilaya_code"],
                "commune_name": normalized["commune_name"],
                "type_risque": normalized["type_risque"],
                "construction_type": normalized["construction_type"],
            },
            "elapsed_ms": round((perf_counter() - started) * 1000, 2),
        }

    def batch_score(self, policies: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self._ensure_loaded()
        if not policies:
            return []
        prepared = self._prepare_policy_frame(policies)
        features, _ = build_feature_matrix(prepared)
        features = self._align_feature_order(features)
        probabilities = self.model.predict_proba(features)

        results = []
        for idx, proba in enumerate(probabilities):
            source_id = policies[idx].get("id") or policies[idx].get("numero_police") or f"API_{idx}"
            results.append(
                {
                    "policy_id": str(source_id),
                    "score": round(float(proba[2]) * 100, 1),
                    "tier": self.tier_labels[int(np.argmax(proba))],
                }
            )
        return results

    def get_feature_importance(self) -> list[FeatureImportanceItem]:
        self._ensure_loaded()
        return self.feature_importance_cache or []

    def _prepare_policy_frame(self, policies: list[dict[str, Any]]) -> pd.DataFrame:
        rows = []
        for idx, policy in enumerate(policies):
            rows.append(
                {
                    "NUMERO_POLICE": policy.get("id") or policy.get("numero_police") or f"API_{idx}",
                    "zone_sismique": policy.get("zone_sismique", "UNKNOWN"),
                    "wilaya_code": policy.get("wilaya_code", "UNKNOWN"),
                    "commune_name": policy.get("commune_name", "UNKNOWN"),
                    "TYPE": policy.get("type_risque", "UNKNOWN"),
                    "construction_type": policy.get("construction_type"),
                    "VALEUR_ASSURÉE": float(policy.get("valeur_assuree", 0)),
                    "PRIME_NETTE": float(policy.get("prime_nette", 0)),
                    "year": int(policy.get("year", 2025)),
                    "DATE_EFFET": policy.get("date_effet"),
                    "DATE_EXPIRATION": policy.get("date_expiration"),
                }
            )
        df = pd.DataFrame(rows)
        return prepare_portfolio_for_model(df, consolidate=False)

    def _align_feature_order(self, features: pd.DataFrame) -> pd.DataFrame:
        if not self.metadata:
            return features
        return features.loc[:, self.metadata["feature_names"]]

    def _get_dominant_factor(self, policy_row: pd.Series) -> str:
        zone = policy_row["zone_sismique"]
        if zone == "III":
            return "seismic_zone"

        adequate_rate = ADEQUATE_RATES.get(zone, 0.002)
        actual_rate = policy_row.get("prime_rate", 0) or 0
        if adequate_rate > 0 and actual_rate > 0 and actual_rate < adequate_rate * 0.75:
            return "premium_adequacy"
        if float(policy_row.get("VALEUR_ASSURÉE", 0)) > 50_000_000:
            return "insured_value"
        if policy_row.get("type_risque") == "INSTALLATION_INDUSTRIELLE":
            return "risk_type"
        return "risk_combination"

    def _ensure_loaded(self) -> None:
        if self.model is None:
            self.load_models()
        if self.model is None:
            raise RuntimeError("CatBoost model is not loaded")


ml_service = MLService()
