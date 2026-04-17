from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
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
        portfolio = await self._load_portfolio_from_db(db)
        result = await asyncio.to_thread(self._run_sync, request, portfolio)
        result["elapsed_seconds"] = round(time.perf_counter() - started, 2)
        return result

    async def _load_portfolio_from_db(self, db: AsyncSession) -> pd.DataFrame:
        result = await db.execute(
            select(
                Policy.id,
                Policy.numero_police,
                Policy.policy_year,
                Policy.type_risque,
                Policy.code_wilaya,
                Policy.wilaya,
                Policy.code_commune,
                Policy.commune,
                Policy.zone_sismique,
                Policy.capital_assure,
                Policy.prime_nette,
                Policy.prime_rate,
                Policy.lat,
                Policy.lon,
            )
        )
        rows = result.all()
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
            return frame

        frame["capital_assure"] = pd.to_numeric(frame["capital_assure"], errors="coerce").fillna(0.0)
        frame = frame[frame["capital_assure"] > 0].copy()
        frame = frame.dropna(subset=["lat", "lon"]).copy()
        frame["wilaya_code"] = frame["wilaya_code"].astype(str).str.zfill(2)
        frame["zone_sismique"] = frame["zone_sismique"].fillna("UNKNOWN")
        return frame

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

    def _run_sync(self, request: SimulationRequest, portfolio_df: pd.DataFrame) -> dict[str, Any]:
        scenario = self._resolve_scenario(request)
        affected = self._get_affected_policies(portfolio_df, scenario, request.scope, request.scope_code)
        if affected.empty:
            return {"error": "No policies in affected area", "affected_policies": 0}

        affected = affected.copy()
        affected["site_pga"] = affected.apply(
            lambda row: self._compute_site_pga(
                float(row["lat"]),
                float(row["lon"]),
                scenario.epicenter,
                scenario.magnitude,
                scenario.depth_km,
            ),
            axis=1,
        )
        affected["mdr"], affected["mdr_sigma"] = zip(
            *affected.apply(
                lambda row: compute_damage_ratio(float(row["site_pga"]), str(row.get("construction_type", "Inconnu"))),
                axis=1,
            )
        )

        n_sims = request.n_simulations or 10_000
        rng = np.random.default_rng(seed=request.seed or 42)
        alpha, beta_params = self._moments_to_beta_params(
            affected["mdr"].to_numpy(dtype=float),
            affected["mdr_sigma"].to_numpy(dtype=float),
        )

        damage_samples = np.zeros((len(affected), n_sims), dtype=float)
        for idx, (alpha_value, beta_value) in enumerate(zip(alpha, beta_params)):
            if alpha_value <= 0 or beta_value <= 0:
                damage_samples[idx] = float(affected.iloc[idx]["mdr"])
            else:
                damage_samples[idx] = rng.beta(alpha_value, beta_value, size=n_sims)

        insured_values = affected["capital_assure"].to_numpy(dtype=float).reshape(-1, 1)
        policy_losses = damage_samples * insured_values
        gross_losses = policy_losses.sum(axis=0)
        net_losses = gross_losses * float(settings.retention_rate)
        mean_policy_losses = policy_losses.mean(axis=1)

        per_commune = self._aggregate_by_commune(affected, mean_policy_losses)
        high_risk_zones = self._aggregate_high_risk_zones(affected, mean_policy_losses)
        overexposed_wilayas = self._aggregate_overexposed_wilayas(affected, mean_policy_losses)

        return {
            "scenario_name": scenario.label,
            "affected_policies": int(len(affected)),
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
            "distribution_json": net_losses[:: max(1, n_sims // 500)].tolist(),
            "per_commune_json": per_commune,
            "high_risk_zones": high_risk_zones,
            "overexposed_wilayas": overexposed_wilayas,
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
            radius_km = 30 * np.exp(0.5 * scenario.magnitude)
            distances = portfolio.apply(
                lambda row: self._haversine_km(
                    float(row["lat"]),
                    float(row["lon"]),
                    scenario.epicenter[0],
                    scenario.epicenter[1],
                ),
                axis=1,
            )
            affected = portfolio[distances <= radius_km].copy()

        if scope == "wilaya" and scope_code:
            affected = affected[affected["wilaya_code"] == scope_code.zfill(2)]
        elif scope == "commune" and scope_code:
            normalized = scope_code.strip().lower()
            affected = affected[
                (affected["code_commune"].astype(str).str.lower() == normalized)
                | (affected["commune_name"].astype(str).str.lower() == normalized)
            ]
        return affected

    def _compute_site_pga(
        self,
        site_lat: float,
        site_lon: float,
        epicenter: tuple[float, float],
        magnitude: float,
        depth_km: float,
    ) -> float:
        distance_epi = self._haversine_km(site_lat, site_lon, epicenter[0], epicenter[1])
        effective_distance = np.sqrt(distance_epi**2 + depth_km**2)
        b1, b2, b3 = 1.647, 0.767, -0.074
        b4, b5 = -2.369, 0.169
        ln_pga = b1 + b2 * magnitude + b3 * magnitude**2 + (b4 + b5 * magnitude) * np.log(max(effective_distance, 1))
        pga_rock = float(np.clip(np.exp(ln_pga), 0.0, 2.0))
        site_factor = 1.3 if distance_epi < 50 else 1.0
        return pga_rock * site_factor

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
        return aggregate.to_dict(orient="records")

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
