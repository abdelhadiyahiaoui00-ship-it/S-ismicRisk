from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.commune import Commune
from app.models.policy import Policy
from app.schemas.simulation import SimulationRequest
from app.services.vulnerability import compute_damage_ratio

TYPE_TO_CONSTRUCTION = {
    "1 - Bien Immobilier": "Maconnerie creuse",
    "2 - Installation Commerciale": "Beton arme",
    "1 - Installation Industrielle": "Structure metallique",
    "3 - Installation Industrielle": "Structure metallique",
}


@dataclass(slots=True)
class ScenarioDefinition:
    label: str
    epicenter: tuple[float, float]
    magnitude: float
    depth_km: float
    affected_wilayas: list[str] | None
    pga_epicenter: float


class SimulationService:
    SCENARIOS: dict[str, ScenarioDefinition] = {
        "boumerdes_2003": ScenarioDefinition(
            label="Boumerdes 2003 - M6.8",
            epicenter=(36.83, 3.65),
            magnitude=6.8,
            depth_km=10.0,
            affected_wilayas=["35", "16", "15", "09", "42", "26"],
            pga_epicenter=0.45,
        ),
        "el_asnam_1980": ScenarioDefinition(
            label="El Asnam 1980 - M7.3",
            epicenter=(36.14, 1.41),
            magnitude=7.3,
            depth_km=10.0,
            affected_wilayas=["02", "14", "27", "38", "45", "22"],
            pga_epicenter=0.65,
        ),
    }

    async def run(self, db: AsyncSession, request: SimulationRequest) -> dict[str, Any]:
        started = time.perf_counter()
        scenario = self._resolve_scenario(request)
        portfolio, quality = await self._load_portfolio_from_db(db, request, scenario)
        result = await asyncio.to_thread(self._run_sync, request, scenario, portfolio, quality)
        result["elapsed_seconds"] = round(time.perf_counter() - started, 2)
        return result

    async def _load_portfolio_from_db(
        self,
        db: AsyncSession,
        request: SimulationRequest,
        scenario: ScenarioDefinition,
    ) -> tuple[pd.DataFrame, dict[str, int | float | str]]:
        commune_match = (
            (Commune.wilaya_code == Policy.code_wilaya)
            & (
                ((Commune.code_commune.is_not(None)) & (Commune.code_commune == Policy.code_commune))
                | (func.lower(Commune.commune_name) == func.lower(Policy.commune))
            )
        )

        query = (
            select(
                Policy.id,
                Policy.numero_police,
                Policy.policy_year,
                Policy.type_risque,
                Policy.code_wilaya,
                Policy.wilaya,
                Policy.code_commune,
                Policy.commune,
                func.coalesce(Commune.zone_sismique, Policy.zone_sismique).label("zone_sismique"),
                Policy.capital_assure,
                Policy.prime_nette,
                Policy.prime_rate,
                func.coalesce(Commune.lat, Policy.lat).label("lat"),
                func.coalesce(Commune.lon, Policy.lon).label("lon"),
            )
            .select_from(Policy)
            .outerjoin(Commune, commune_match)
            .where(Policy.capital_assure > 0)
        )

        if request.scope == "wilaya" and request.scope_code:
            query = query.where(Policy.code_wilaya == request.scope_code.zfill(2))
        elif request.scope == "commune" and request.scope_code:
            normalized = request.scope_code.strip().lower()
            query = query.where(
                (func.lower(Policy.code_commune) == normalized)
                | (func.lower(Policy.commune) == normalized)
                | (func.lower(Commune.code_commune) == normalized)
                | (func.lower(Commune.commune_name) == normalized)
            )

        if scenario.affected_wilayas:
            query = query.where(Policy.code_wilaya.in_(scenario.affected_wilayas))

        result = await db.execute(
            query
        )
        rows = result.all()
        source_policies = len(rows)
        data = []
        for row in rows:
            data.append(
                {
                    "policy_id": row.id,
                    "numero_police": row.numero_police,
                    "policy_year": row.policy_year,
                    "TYPE": row.type_risque,
                    "type_risque": row.type_risque,
                    "wilaya_code": row.code_wilaya,
                    "wilaya_name": row.wilaya,
                    "code_commune": row.code_commune,
                    "commune_name": row.commune,
                    "zone_sismique": row.zone_sismique,
                    "capital_assure": float(row.capital_assure or 0),
                    "prime_nette": float(row.prime_nette or 0),
                    "prime_rate": float(row.prime_rate) if row.prime_rate is not None else None,
                    "lat": float(row.lat) if row.lat is not None else None,
                    "lon": float(row.lon) if row.lon is not None else None,
                    "construction_type": TYPE_TO_CONSTRUCTION.get(row.type_risque, "Inconnu"),
                }
            )

        frame = pd.DataFrame(data)
        if frame.empty:
            return frame, {
                "source_policies": 0,
                "cleaned_policies": 0,
                "dropped_missing_coordinates": 0,
                "dropped_unknown_zone": 0,
                "dropped_out_of_bounds": 0,
                "deduplicated_rows": 0,
            }

        frame["capital_assure"] = pd.to_numeric(frame["capital_assure"], errors="coerce").fillna(0.0)
        frame = frame[frame["capital_assure"] > 0].copy()
        before_missing = len(frame)
        frame = frame.dropna(subset=["lat", "lon"]).copy()
        dropped_missing_coordinates = before_missing - len(frame)
        frame["wilaya_code"] = frame["wilaya_code"].astype(str).str.zfill(2)
        frame["zone_sismique"] = frame["zone_sismique"].fillna("UNKNOWN")
        before_zone = len(frame)
        frame = frame[frame["zone_sismique"].isin({"0", "I", "IIa", "IIb", "III"})].copy()
        dropped_unknown_zone = before_zone - len(frame)
        before_bounds = len(frame)
        frame = frame[
            frame["lat"].between(18.0, 38.5)
            & frame["lon"].between(-9.5, 12.5)
        ].copy()
        dropped_out_of_bounds = before_bounds - len(frame)
        before_dedup = len(frame)
        frame = frame.drop_duplicates(subset=["policy_id"]).copy()
        deduplicated_rows = before_dedup - len(frame)
        quality = {
            "source_policies": source_policies,
            "cleaned_policies": int(len(frame)),
            "dropped_missing_coordinates": int(dropped_missing_coordinates),
            "dropped_unknown_zone": int(dropped_unknown_zone),
            "dropped_out_of_bounds": int(dropped_out_of_bounds),
            "deduplicated_rows": int(deduplicated_rows),
        }
        return frame, quality

    def list_scenarios(self) -> dict[str, dict[str, Any]]:
        return {
            key: {
                "label": value.label,
                "magnitude": value.magnitude,
                "epicenter": value.epicenter,
                "depth_km": value.depth_km,
                "affected_wilayas": value.affected_wilayas or [],
            }
            for key, value in self.SCENARIOS.items()
        }

    def _run_sync(
        self,
        request: SimulationRequest,
        scenario: ScenarioDefinition,
        portfolio_df: pd.DataFrame,
        quality: dict[str, int | float | str],
    ) -> dict[str, Any]:
        affected = self._get_affected_policies(portfolio_df, scenario, request.scope, request.scope_code)
        if affected.empty:
            return {"error": "No policies in affected area", "affected_policies": 0}

        affected = affected.copy()
        site_pga = self._compute_site_pga_vectorized(
            affected["lat"].to_numpy(dtype=float),
            affected["lon"].to_numpy(dtype=float),
            scenario.epicenter,
            scenario.magnitude,
            scenario.depth_km,
        )
        affected["site_pga"] = np.clip(site_pga, 0.0, 1.5)
        mdr_values: list[float] = []
        sigma_values: list[float] = []
        for pga, construction_type in zip(
            affected["site_pga"].to_numpy(dtype=float),
            affected["construction_type"].astype(str).to_numpy(),
        ):
            mean_ratio, sigma = compute_damage_ratio(float(pga), construction_type)
            mdr_values.append(mean_ratio)
            sigma_values.append(sigma)
        affected["mdr"] = np.clip(np.asarray(mdr_values, dtype=float), 0.0, 0.95)
        affected["mdr_sigma"] = np.clip(np.asarray(sigma_values, dtype=float), 0.005, 0.20)

        n_sims = min(request.n_simulations or 3_000, 20_000)
        rng = np.random.default_rng(seed=request.seed or 42)
        alpha, beta_params = self._moments_to_beta_params(
            affected["mdr"].to_numpy(dtype=float),
            affected["mdr_sigma"].to_numpy(dtype=float),
        )

        gross_losses = np.zeros(n_sims, dtype=np.float64)
        mean_policy_losses = np.zeros(len(affected), dtype=np.float64)

        insured_values = affected["capital_assure"].to_numpy(dtype=float)
        mean_damage_ratios = affected["mdr"].to_numpy(dtype=float)

        for idx, (alpha_value, beta_value, insured_value, mean_damage_ratio) in enumerate(
            zip(alpha, beta_params, insured_values, mean_damage_ratios)
        ):
            if alpha_value <= 0 or beta_value <= 0:
                sampled_ratios = np.full(n_sims, mean_damage_ratio, dtype=np.float64)
            else:
                sampled_ratios = rng.beta(alpha_value, beta_value, size=n_sims)
            lower = max(0.0, mean_damage_ratio - 3 * float(affected.iloc[idx]["mdr_sigma"]))
            upper = min(0.98, mean_damage_ratio + 3 * float(affected.iloc[idx]["mdr_sigma"]))
            sampled_ratios = np.clip(sampled_ratios, lower, upper)
            gross_losses += sampled_ratios * insured_value
            mean_policy_losses[idx] = mean_damage_ratio * insured_value

        net_losses = gross_losses * float(settings.retention_rate)

        per_commune = self._aggregate_by_commune(affected, mean_policy_losses)
        high_risk_zones = self._aggregate_high_risk_zones(affected, mean_policy_losses)
        overexposed_wilayas = self._aggregate_overexposed_wilayas(affected, mean_policy_losses)

        return {
            "scenario_name": scenario.label,
            "affected_policies": int(len(affected)),
            "source_policies": int(quality.get("source_policies", len(portfolio_df))),
            "cleaned_policies": int(quality.get("cleaned_policies", len(portfolio_df))),
            "n_simulations": int(n_sims),
            "expected_loss": float(net_losses.mean()),
            "expected_gross_loss": float(gross_losses.mean()),
            "gross_var_95": float(np.percentile(gross_losses, 95)),
            "gross_var_99": float(np.percentile(gross_losses, 99)),
            "expected_net_loss": float(net_losses.mean()),
            "var_95": float(np.percentile(net_losses, 95)),
            "var_99": float(np.percentile(net_losses, 99)),
            "pml_999": float(np.percentile(net_losses, 99.9)),
            "worst_case_loss": float(net_losses.max()),
            "per_commune_json": per_commune,
            "high_risk_zones": high_risk_zones,
            "overexposed_wilayas": overexposed_wilayas,
            "data_quality": quality,
        }

    def _resolve_scenario(self, request: SimulationRequest) -> ScenarioDefinition:
        if request.scenario == "custom":
            return ScenarioDefinition(
                label=f"Custom M{request.magnitude:.1f}",
                epicenter=(request.epicenter_lat, request.epicenter_lon),
                magnitude=request.magnitude,
                depth_km=request.depth_km,
                affected_wilayas=None,
                pga_epicenter=self._magnitude_to_pga(request.magnitude),
            )
        return self.SCENARIOS[request.scenario]

    def _get_affected_policies(
        self,
        portfolio: pd.DataFrame,
        scenario: ScenarioDefinition,
        scope: str | None,
        scope_code: str | None,
    ) -> pd.DataFrame:
        if scenario.affected_wilayas:
            affected = portfolio[portfolio["wilaya_code"].isin(scenario.affected_wilayas)].copy()
        else:
            affected = portfolio.copy()

        radius_km = self._impact_radius_km(scenario.magnitude)
        distances = self._haversine_km_vectorized(
            affected["lat"].to_numpy(dtype=float),
            affected["lon"].to_numpy(dtype=float),
            scenario.epicenter[0],
            scenario.epicenter[1],
        )
        affected = affected[distances <= radius_km].copy()
        affected["distance_km"] = distances[distances <= radius_km]

        if scope == "wilaya" and scope_code:
            affected = affected[affected["wilaya_code"] == scope_code.zfill(2)]
        elif scope == "commune" and scope_code:
            normalized = scope_code.strip().lower()
            affected = affected[
                (affected["code_commune"].astype(str).str.lower() == normalized)
                | (affected["commune_name"].astype(str).str.lower() == normalized)
            ]
        return affected

    def _compute_site_pga_vectorized(
        self,
        site_lat: np.ndarray,
        site_lon: np.ndarray,
        epicenter: tuple[float, float],
        magnitude: float,
        depth_km: float,
    ) -> np.ndarray:
        distance_epi = self._haversine_km_vectorized(site_lat, site_lon, epicenter[0], epicenter[1])
        effective_distance = np.sqrt(distance_epi**2 + depth_km**2)
        b1, b2, b3 = 1.647, 0.767, -0.074
        b4, b5 = -2.369, 0.169
        ln_pga = b1 + b2 * magnitude + b3 * magnitude**2 + (b4 + b5 * magnitude) * np.log(np.maximum(effective_distance, 1))
        pga_rock = np.clip(np.exp(ln_pga), 0.0, 2.0)
        site_factor = np.where(distance_epi < 50, 1.3, 1.0)
        return pga_rock * site_factor

    def _haversine_km_vectorized(
        self,
        lat1: np.ndarray,
        lon1: np.ndarray,
        lat2: float,
        lon2: float,
    ) -> np.ndarray:
        earth_radius_km = 6371.0
        dlat = np.radians(lat2 - lat1)
        dlon = np.radians(lon2 - lon1)
        a = (
            np.sin(dlat / 2) ** 2
            + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2) ** 2
        )
        return earth_radius_km * 2 * np.arcsin(np.sqrt(a))

    def _impact_radius_km(self, magnitude: float) -> float:
        return float(np.clip(25 * np.exp(0.42 * magnitude), 80.0, 420.0))

    def _compute_site_pga(
        self,
        site_lat: float,
        site_lon: float,
        epicenter: tuple[float, float],
        magnitude: float,
        depth_km: float,
    ) -> float:
        return float(
            self._compute_site_pga_vectorized(
                np.asarray([site_lat], dtype=float),
                np.asarray([site_lon], dtype=float),
                epicenter,
                magnitude,
                depth_km,
            )[0]
        )

    def _moments_to_beta_params(self, means: np.ndarray, stds: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        means = np.clip(means, 0.001, 0.999)
        stds = np.clip(stds, 0.001, 0.5)
        variance = stds**2
        common = means * (1 - means) / variance - 1
        alpha = np.maximum(means * common, 0.1)
        beta_params = np.maximum((1 - means) * common, 0.1)
        return alpha, beta_params

    def _aggregate_by_commune(self, affected: pd.DataFrame, mean_losses: np.ndarray) -> list[dict[str, Any]]:
        frame = affected.copy()
        frame["expected_loss"] = mean_losses
        aggregate = (
            frame.groupby(["wilaya_code", "wilaya_name", "code_commune", "commune_name", "zone_sismique", "lat", "lon"])
            .agg(
                expected_loss=("expected_loss", "sum"),
                policy_count=("capital_assure", "count"),
                total_exposure=("capital_assure", "sum"),
            )
            .reset_index()
            .sort_values("expected_loss", ascending=False)
        )
        return aggregate.head(100).to_dict(orient="records")

    def _aggregate_high_risk_zones(self, affected: pd.DataFrame, mean_losses: np.ndarray) -> list[dict[str, Any]]:
        frame = affected.copy()
        frame["expected_loss"] = mean_losses
        aggregate = (
            frame.groupby(["zone_sismique"])
            .agg(
                expected_loss=("expected_loss", "sum"),
                policy_count=("capital_assure", "count"),
                total_exposure=("capital_assure", "sum"),
            )
            .reset_index()
            .sort_values("expected_loss", ascending=False)
        )
        return aggregate.head(5).to_dict(orient="records")

    def _aggregate_overexposed_wilayas(self, affected: pd.DataFrame, mean_losses: np.ndarray) -> list[dict[str, Any]]:
        frame = affected.copy()
        frame["expected_loss"] = mean_losses
        aggregate = (
            frame.groupby(["wilaya_code", "wilaya_name"])
            .agg(
                expected_loss=("expected_loss", "sum"),
                policy_count=("capital_assure", "count"),
                total_exposure=("capital_assure", "sum"),
            )
            .reset_index()
            .sort_values("expected_loss", ascending=False)
        )
        return aggregate.head(5).to_dict(orient="records")

    @staticmethod
    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        earth_radius_km = 6371.0
        dlat = np.radians(lat2 - lat1)
        dlon = np.radians(lon2 - lon1)
        a = (
            np.sin(dlat / 2) ** 2
            + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2) ** 2
        )
        return float(earth_radius_km * 2 * np.arcsin(np.sqrt(a)))

    @staticmethod
    def _magnitude_to_pga(magnitude: float) -> float:
        return float(min(0.15 * np.exp(0.6 * (magnitude - 5.0)), 2.0))


simulation_service = SimulationService()
