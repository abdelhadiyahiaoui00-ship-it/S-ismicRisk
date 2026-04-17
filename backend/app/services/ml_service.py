from __future__ import annotations

from collections import Counter, defaultdict
import json
import pickle
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.policy import Policy
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
        self._score_analytics_cache_key: str | None = None
        self._score_analytics_cache: dict[str, Any] | None = None

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
        chunk_results: list[dict[str, Any]] = []
        for offset in range(0, len(policies), 2000):
            chunk = policies[offset : offset + 2000]
            prepared = self._prepare_policy_frame(chunk)
            features, _ = build_feature_matrix(prepared)
            features = self._align_feature_order(features)
            probabilities = self.model.predict_proba(features)

            for idx, proba in enumerate(probabilities):
                source = chunk[idx]
                source_id = source.get("id") or source.get("numero_police") or f"API_{offset + idx}"
                chunk_results.append(
                    {
                        "policy_id": str(source_id),
                        "score": round(float(proba[2]) * 100, 1),
                        "tier": self.tier_labels[int(np.argmax(proba))],
                    }
                )
        return chunk_results

    async def get_portfolio_score_analytics(self, db: AsyncSession) -> dict[str, Any]:
        self._ensure_loaded()
        cache_row = await db.execute(select(func.max(Policy.updated_at), func.count(Policy.id)))
        max_updated_at, policy_count = cache_row.one()
        cache_key = f"{max_updated_at.isoformat() if max_updated_at else 'none'}::{policy_count}"
        if self._score_analytics_cache_key == cache_key and self._score_analytics_cache is not None:
            return self._score_analytics_cache

        result = await db.execute(
            select(
                Policy.id,
                Policy.numero_police,
                Policy.zone_sismique,
                Policy.code_wilaya,
                Policy.wilaya,
                Policy.code_commune,
                Policy.commune,
                Policy.type_risque,
                Policy.capital_assure,
                Policy.prime_nette,
                Policy.policy_year,
                Policy.date_effet,
                Policy.date_expiration,
            )
        )
        policies = [
            {
                "id": policy_id,
                "numero_police": numero_police,
                "zone_sismique": zone_sismique,
                "wilaya_code": code_wilaya,
                "wilaya_name": wilaya,
                "code_commune": code_commune,
                "commune_name": commune,
                "type_risque": type_risque,
                "valeur_assuree": float(capital_assure or 0),
                "prime_nette": float(prime_nette or 0),
                "year": int(policy_year or 2025),
                "date_effet": date_effet.isoformat() if date_effet else None,
                "date_expiration": date_expiration.isoformat() if date_expiration else None,
            }
            for (
                policy_id,
                numero_police,
                zone_sismique,
                code_wilaya,
                wilaya,
                code_commune,
                commune,
                type_risque,
                capital_assure,
                prime_nette,
                policy_year,
                date_effet,
                date_expiration,
            ) in result.all()
        ]

        analytics = self._compute_score_analytics(policies)
        self._score_analytics_cache_key = cache_key
        self._score_analytics_cache = analytics
        return analytics

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

    def _compute_score_analytics(self, policies: list[dict[str, Any]]) -> dict[str, Any]:
        if not policies:
            return {
                "high_count": 0,
                "medium_count": 0,
                "low_count": 0,
                "high_pct": 0.0,
                "avg_score": 0.0,
                "dominant_factor": None,
                "commune_average_scores": {},
                "top_high_risk_communes": [],
            }

        factor_counts: Counter[str] = Counter()
        commune_buckets: dict[tuple[str, str], dict[str, Any]] = defaultdict(
            lambda: {
                "score_sum": 0.0,
                "count": 0,
                "high_count": 0,
                "wilaya_code": None,
                "wilaya_name": None,
                "commune_code": None,
                "commune_name": None,
            }
        )
        high_count = 0
        medium_count = 0
        low_count = 0
        score_sum = 0.0

        for offset in range(0, len(policies), 2000):
            chunk = policies[offset : offset + 2000]
            prepared = self._prepare_policy_frame(chunk)
            features, _ = build_feature_matrix(prepared)
            features = self._align_feature_order(features)
            probabilities = self.model.predict_proba(features)

            for idx, proba in enumerate(probabilities):
                score = float(proba[2]) * 100
                tier = self.tier_labels[int(np.argmax(proba))]
                normalized = prepared.iloc[idx]
                factor = self._get_dominant_factor(normalized)
                factor_counts[factor] += 1
                score_sum += score

                if tier == "HIGH":
                    high_count += 1
                elif tier == "MEDIUM":
                    medium_count += 1
                else:
                    low_count += 1

                source = chunk[idx]
                commune_key = (
                    str(source.get("wilaya_code") or "").zfill(2),
                    str(source.get("commune_name") or "").strip().lower(),
                )
                bucket = commune_buckets[commune_key]
                bucket["score_sum"] += score
                bucket["count"] += 1
                bucket["high_count"] += 1 if tier == "HIGH" else 0
                bucket["wilaya_code"] = str(source.get("wilaya_code") or "").zfill(2)
                bucket["wilaya_name"] = source.get("wilaya_name")
                bucket["commune_code"] = source.get("code_commune")
                bucket["commune_name"] = source.get("commune_name")

        commune_average_scores: dict[tuple[str, str], dict[str, Any]] = {}
        for key, bucket in commune_buckets.items():
            avg_score = bucket["score_sum"] / max(bucket["count"], 1)
            commune_average_scores[key] = {
                "avg_score": round(avg_score, 2),
                "policy_count": bucket["count"],
                "high_count": bucket["high_count"],
                "high_pct": round(bucket["high_count"] * 100 / max(bucket["count"], 1), 2),
                "wilaya_code": bucket["wilaya_code"],
                "wilaya_name": bucket["wilaya_name"],
                "commune_code": bucket["commune_code"],
                "commune_name": bucket["commune_name"],
            }

        top_high_risk_communes = sorted(
            commune_average_scores.values(),
            key=lambda item: (item["avg_score"], item["high_count"], item["policy_count"]),
            reverse=True,
        )[:10]

        return {
            "high_count": high_count,
            "medium_count": medium_count,
            "low_count": low_count,
            "high_pct": round(high_count * 100 / max(len(policies), 1), 2),
            "avg_score": round(score_sum / max(len(policies), 1), 2),
            "dominant_factor": factor_counts.most_common(1)[0][0] if factor_counts else None,
            "commune_average_scores": commune_average_scores,
            "top_high_risk_communes": top_high_risk_communes,
        }

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
