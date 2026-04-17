from __future__ import annotations

import json
import math
import re
import unicodedata
from difflib import get_close_matches
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from pathlib import Path

WILAYA_NAMES: dict[str, str] = {
    "01": "ADRAR",
    "02": "CHLEF",
    "03": "LAGHOUAT",
    "04": "OUM EL BOUAGHI",
    "05": "BATNA",
    "06": "BEJAIA",
    "07": "BISKRA",
    "08": "BECHAR",
    "09": "BLIDA",
    "10": "BOUIRA",
    "11": "TAMANRASSET",
    "12": "TEBESSA",
    "13": "TLEMCEN",
    "14": "TIARET",
    "15": "TIZI OUZOU",
    "16": "ALGER",
    "17": "DJELFA",
    "18": "JIJEL",
    "19": "SETIF",
    "20": "SAIDA",
    "21": "SKIKDA",
    "22": "SIDI BEL ABBES",
    "23": "ANNABA",
    "24": "GUELMA",
    "25": "CONSTANTINE",
    "26": "MEDEA",
    "27": "MOSTAGANEM",
    "28": "M SILA",
    "29": "MASCARA",
    "30": "OUARGLA",
    "31": "ORAN",
    "32": "EL BAYADH",
    "33": "ILLIZI",
    "34": "BORDJ BOU ARRERIDJ",
    "35": "BOUMERDES",
    "36": "EL TAREF",
    "37": "TINDOUF",
    "38": "TISSEMSILT",
    "39": "EL OUED",
    "40": "KHENCHELA",
    "41": "SOUK AHRAS",
    "42": "TIPAZA",
    "43": "MILA",
    "44": "AIN DEFLA",
    "45": "NAAMA",
    "46": "AIN TEMOUCHENT",
    "47": "GHARDAIA",
    "48": "RELIZANE",
    "49": "TIMIMOUN",
    "50": "BORDJ BADJI MOKHTAR",
    "51": "OULED DJELLAL",
    "52": "BENI ABBES",
    "53": "IN SALAH",
    "54": "IN GUEZZAM",
    "55": "TOUGGOURT",
    "56": "DJANET",
    "57": "EL M GHAIR",
    "58": "EL MENIAA",
}


def normalize_label(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    text = text.upper().replace("'", " ").replace("-", " ")
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_decimal(value: str | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    return Decimal(raw)


def _haversine_km(lat1: Decimal, lon1: Decimal, lat2: Decimal, lon2: Decimal) -> float:
    radius_km = 6371.0
    lat1_f = math.radians(float(lat1))
    lon1_f = math.radians(float(lon1))
    lat2_f = math.radians(float(lat2))
    lon2_f = math.radians(float(lon2))

    d_lat = lat2_f - lat1_f
    d_lon = lon2_f - lon1_f
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(lat1_f) * math.cos(lat2_f) * math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_km * c


@dataclass(frozen=True)
class CanonicalCommune:
    code_commune: str
    commune_name: str
    normalized_name: str
    wilaya_code: str
    wilaya_name: str
    lat: Decimal | None
    lon: Decimal | None


@dataclass(frozen=True)
class ResolvedCommune:
    commune: CanonicalCommune
    method: str
    distance_km: float | None = None


class AlgeriaLocationReference:
    def __init__(self, path: Path | None = None) -> None:
        reference_path = path or Path(__file__).resolve().parents[1] / "data" / "algeria_communes_reference.json"
        payload = json.loads(reference_path.read_text(encoding="utf-8"))
        self.communes: list[CanonicalCommune] = []
        self.by_name: dict[str, list[CanonicalCommune]] = {}
        self.by_wilaya: dict[str, list[CanonicalCommune]] = {}
        self.normalized_names_by_wilaya: dict[str, list[str]] = {}

        for row in payload:
            wilaya_code = str(row["wilaya_id"]).zfill(2)
            commune = CanonicalCommune(
                code_commune=str(row["post_code"]).strip(),
                commune_name=str(row["name_fr"]).strip().upper(),
                normalized_name=normalize_label(row["name_fr"]),
                wilaya_code=wilaya_code,
                wilaya_name=WILAYA_NAMES.get(wilaya_code, f"WILAYA {wilaya_code}"),
                lat=_parse_decimal(row.get("latitude")),
                lon=_parse_decimal(row.get("longitude")),
            )
            self.communes.append(commune)
            self.by_name.setdefault(commune.normalized_name, []).append(commune)
            self.by_wilaya.setdefault(commune.wilaya_code, []).append(commune)

        for wilaya_code, communes in self.by_wilaya.items():
            self.normalized_names_by_wilaya[wilaya_code] = sorted({item.normalized_name for item in communes})

    def _name_variants(
        self,
        commune_name: str | None,
        *,
        wilaya_name: str | None = None,
        raw_label: str | None = None,
    ) -> list[str]:
        labels = [commune_name or ""]
        if raw_label and " - " in raw_label:
            labels.append(raw_label.split(" - ", 1)[1])
        elif raw_label:
            labels.append(raw_label)

        wilaya_tokens = {normalize_label(wilaya_name)}
        wilaya_tokens.update(normalize_label(name) for name in WILAYA_NAMES.values())
        wilaya_tokens.discard("")

        variants: list[str] = []
        for label in labels:
            normalized = normalize_label(label)
            if not normalized:
                continue
            variants.append(normalized)
            for token in wilaya_tokens:
                suffix = f" {token}"
                if normalized.endswith(suffix) and normalized != token:
                    variants.append(normalized[: -len(suffix)].strip())
            if normalized == "ALGER":
                variants.append("ALGER CENTRE")

        deduped: list[str] = []
        seen: set[str] = set()
        for item in variants:
            if item and item not in seen:
                deduped.append(item)
                seen.add(item)
        return deduped

    def _pick_best_candidate(
        self,
        candidates: list[CanonicalCommune],
        *,
        wilaya_code: str | None,
        lat: Decimal | None,
        lon: Decimal | None,
    ) -> ResolvedCommune | None:
        if not candidates:
            return None

        if len(candidates) == 1:
            return ResolvedCommune(commune=candidates[0], method="canonical_unique_name")

        if wilaya_code:
            matching_wilaya = [item for item in candidates if item.wilaya_code == wilaya_code]
            if len(matching_wilaya) == 1:
                return ResolvedCommune(commune=matching_wilaya[0], method="canonical_wilaya_match")
            if matching_wilaya:
                candidates = matching_wilaya

        if lat is not None and lon is not None:
            nearest: tuple[CanonicalCommune, float] | None = None
            for item in candidates:
                if item.lat is None or item.lon is None:
                    continue
                distance = _haversine_km(lat, lon, item.lat, item.lon)
                if nearest is None or distance < nearest[1]:
                    nearest = (item, distance)
            if nearest is not None:
                return ResolvedCommune(
                    commune=nearest[0],
                    method="canonical_coordinate_match",
                    distance_km=nearest[1],
                )

        return ResolvedCommune(commune=candidates[0], method="canonical_ambiguous_first")

    def resolve(
        self,
        commune_name: str | None,
        *,
        wilaya_code: str | None = None,
        wilaya_name: str | None = None,
        lat: Decimal | str | None = None,
        lon: Decimal | str | None = None,
        raw_label: str | None = None,
    ) -> ResolvedCommune | None:
        normalized_wilaya_code = str(wilaya_code).zfill(2) if wilaya_code else None
        lat_value = _parse_decimal(lat)
        lon_value = _parse_decimal(lon)

        for variant in self._name_variants(commune_name, wilaya_name=wilaya_name, raw_label=raw_label):
            match = self._pick_best_candidate(
                self.by_name.get(variant, []),
                wilaya_code=normalized_wilaya_code,
                lat=lat_value,
                lon=lon_value,
            )
            if match is not None:
                return match

        if normalized_wilaya_code:
            wilaya_names = self.normalized_names_by_wilaya.get(normalized_wilaya_code, [])
            for variant in self._name_variants(commune_name, wilaya_name=wilaya_name, raw_label=raw_label):
                close = get_close_matches(variant, wilaya_names, n=1, cutoff=0.82)
                if close:
                    match = self._pick_best_candidate(
                        self.by_name.get(close[0], []),
                        wilaya_code=normalized_wilaya_code,
                        lat=lat_value,
                        lon=lon_value,
                    )
                    if match is not None:
                        return ResolvedCommune(
                            commune=match.commune,
                            method="canonical_fuzzy_wilaya_name",
                            distance_km=match.distance_km,
                        )

        if lat_value is None or lon_value is None:
            return None

        scoped_candidates = self.by_wilaya.get(normalized_wilaya_code or "", [])
        if scoped_candidates:
            nearest = self._pick_best_candidate(
                scoped_candidates,
                wilaya_code=normalized_wilaya_code,
                lat=lat_value,
                lon=lon_value,
            )
            if nearest is not None and nearest.distance_km is not None and nearest.distance_km <= 15:
                return ResolvedCommune(
                    commune=nearest.commune,
                    method="canonical_coordinate_fallback_same_wilaya",
                    distance_km=nearest.distance_km,
                )

        nearest_any = self._pick_best_candidate(
            self.communes,
            wilaya_code=None,
            lat=lat_value,
            lon=lon_value,
        )
        if nearest_any is not None and nearest_any.distance_km is not None and nearest_any.distance_km <= 8:
            return ResolvedCommune(
                commune=nearest_any.commune,
                method="canonical_coordinate_fallback_any_wilaya",
                distance_km=nearest_any.distance_km,
            )
        return None


@lru_cache(maxsize=1)
def get_algeria_location_reference() -> AlgeriaLocationReference:
    return AlgeriaLocationReference()
