from __future__ import annotations

import io
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import torch
import torch.nn as nn
import torchvision.transforms as T
from PIL import Image
from torchvision.models import mobilenet_v3_small

from app.core.config import settings


def build_model() -> nn.Module:
    model = mobilenet_v3_small(weights=None)
    in_features = model.classifier[-1].in_features
    model.classifier[-1] = nn.Linear(in_features, 4)
    return model


CONSTRUCTION_ALIASES = {
    "MACONNERIE CREUSE": "Maconnerie creuse",
    "MA CONNERIE CREUSE": "Maconnerie creuse",
    "MACONNERIE CHAINEE": "Maconnerie chainee",
    "BETON ARME": "Beton arme",
    "B ETON ARME": "Beton arme",
    "STRUCTURE METALLIQUE": "Structure metallique",
    "STRUCTURE METALLIC": "Structure metallique",
}


class DamageAIService:
    DAMAGE_CLASSES = {0: "No Damage", 1: "Minor Damage", 2: "Major Damage", 3: "Destroyed"}
    LOSS_MULTIPLIERS = {0: 0.0, 1: 0.10, 2: 0.45, 3: 0.85}
    MAX_INFERENCE_DIM = 512
    MAX_HEATMAP_DIM = 960
    PATCH_SIZE = 64
    STRIDE = 64
    DAMAGE_COLORS = {
        0: np.array([0, 255, 0], dtype=np.uint8),
        1: np.array([255, 255, 0], dtype=np.uint8),
        2: np.array([255, 140, 0], dtype=np.uint8),
        3: np.array([255, 0, 0], dtype=np.uint8),
    }

    def __init__(self) -> None:
        self.model: nn.Module | None = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model_path = Path(__file__).resolve().parents[1] / "ml_models" / Path(settings.damage_cnn_path).name
        self.heatmaps_dir = Path(__file__).resolve().parents[2] / settings.heatmaps_dir
        self.transform = T.Compose(
            [
                T.Resize((64, 64)),
                T.ToTensor(),
                T.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
            ]
        )

    def load_model(self) -> None:
        if self.model is not None:
            return
        if not self.model_path.exists():
            return

        model = build_model()
        state_dict = torch.load(self.model_path, map_location=self.device)
        model.load_state_dict(state_dict)
        model.to(self.device)
        model.eval()
        self.model = model

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "model_loaded": self.model is not None,
            "device": str(self.device),
            "model_path": str(self.model_path),
        }

    def estimate_damage(
        self,
        *,
        image_bytes: bytes,
        image_type: str = "satellite",
        area_km2: float = 1.0,
        construction_type: str = "Beton arme",
        zone_sismique: str = "IIb",
        wilaya_code: str | None = None,
        commune_name: str | None = None,
    ) -> dict[str, Any]:
        area_km2 = float(np.clip(area_km2 or 1.0, 0.05, 500.0))
        construction_type = self._normalize_construction_type(construction_type)
        zone_sismique = self._normalize_zone(zone_sismique)

        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        image = self._prepare_image(image, max_dim=self.MAX_HEATMAP_DIM)
        img_array = np.array(image)

        if self.model is not None:
            breakdown, damage_map = self._run_model(image)
            confidence = 0.78
            is_mock = False
        else:
            breakdown, damage_map = self._mock_estimate(zone_sismique, construction_type, image.size)
            confidence = 0.0
            is_mock = True

        dominant_class = max(
            self.DAMAGE_CLASSES.keys(),
            key=lambda idx: breakdown.get(self.DAMAGE_CLASSES[idx], 0.0),
        )
        loss_percentage = sum(
            breakdown.get(label, 0.0) * self.LOSS_MULTIPLIERS[class_idx]
            for class_idx, label in self.DAMAGE_CLASSES.items()
        )
        avg_value = self._avg_value_per_km2(construction_type)
        loss_per_km2 = loss_percentage * avg_value
        total_loss = loss_per_km2 * area_km2
        heatmap_url = self._generate_heatmap(img_array, damage_map)

        return {
            "damage_class": int(dominant_class),
            "damage_label": self.DAMAGE_CLASSES[dominant_class],
            "loss_percentage": round(float(loss_percentage), 4),
            "loss_per_km2_dzd": round(float(loss_per_km2), 0),
            "total_loss_dzd": round(float(total_loss), 0),
            "confidence": round(float(confidence), 3),
            "is_mock": is_mock,
            "heatmap_url": heatmap_url,
            "affected_area_km2": round(area_km2, 3),
            "breakdown": {label: round(float(value), 3) for label, value in breakdown.items()},
            "image_type": image_type,
            "construction_type": construction_type,
            "zone_sismique": zone_sismique,
            "wilaya_code": wilaya_code,
            "commune_name": commune_name,
        }

    def _run_model(self, image: Image.Image) -> tuple[dict[str, float], np.ndarray]:
        assert self.model is not None
        inference_image = self._prepare_image(image, max_dim=self.MAX_INFERENCE_DIM)
        width, height = inference_image.size
        patch_size = self.PATCH_SIZE
        stride = self.STRIDE

        patches: list[torch.Tensor] = []
        coords: list[tuple[int, int, int, int]] = []
        y_positions = list(range(0, max(height - patch_size + 1, 1), stride)) or [0]
        x_positions = list(range(0, max(width - patch_size + 1, 1), stride)) or [0]
        if y_positions[-1] != max(0, height - patch_size):
            y_positions.append(max(0, height - patch_size))
        if x_positions[-1] != max(0, width - patch_size):
            x_positions.append(max(0, width - patch_size))

        seen: set[tuple[int, int]] = set()
        for y in y_positions:
            for x in x_positions:
                key = (y, x)
                if key in seen:
                    continue
                seen.add(key)
                crop = inference_image.crop((x, y, min(x + patch_size, width), min(y + patch_size, height))).resize((64, 64))
                patches.append(self.transform(crop))
                coords.append((y, min(y + patch_size, height), x, min(x + patch_size, width)))

        preds_batches: list[np.ndarray] = []
        with torch.no_grad():
            for start in range(0, len(patches), 128):
                batch = torch.stack(patches[start:start + 128]).to(self.device)
                logits = self.model(batch)
                probabilities = torch.softmax(logits, dim=1).cpu().numpy()
                preds_batches.append(probabilities.argmax(axis=1))
        preds = np.concatenate(preds_batches) if preds_batches else np.array([], dtype=np.int64)

        damage_map = np.zeros((height, width), dtype=np.float32)
        count_map = np.zeros((height, width), dtype=np.float32)
        for pred, (y1, y2, x1, x2) in zip(preds, coords):
            damage_map[y1:y2, x1:x2] += pred
            count_map[y1:y2, x1:x2] += 1
        count_map = np.where(count_map == 0, 1, count_map)
        damage_map = damage_map / count_map

        total = max(len(preds), 1)
        breakdown = {
            self.DAMAGE_CLASSES[idx]: float((preds == idx).sum()) / total
            for idx in self.DAMAGE_CLASSES
        }
        return breakdown, damage_map

    def _mock_estimate(
        self,
        zone: str,
        construction_type: str,
        image_size: tuple[int, int],
    ) -> tuple[dict[str, float], np.ndarray]:
        zone_severity = {"0": 0.05, "I": 0.18, "IIa": 0.34, "IIb": 0.55, "III": 0.78}
        vulnerability = {
            "Maconnerie creuse": 0.85,
            "Maconnerie chainee": 0.55,
            "Beton arme": 0.35,
            "Structure metallique": 0.20,
        }.get(construction_type, 0.60)
        combined = np.clip(zone_severity.get(zone, 0.30) * 0.6 + vulnerability * 0.4, 0.05, 0.95)
        breakdown = {
            "No Damage": max(0.02, 1.0 - combined * 1.1),
            "Minor Damage": min(0.38, combined * 0.45),
            "Major Damage": min(0.34, combined * 0.42),
            "Destroyed": max(0.01, combined * 0.22),
        }
        total = sum(breakdown.values())
        breakdown = {k: v / total for k, v in breakdown.items()}

        width, height = image_size
        yy, xx = np.mgrid[0:height, 0:width]
        center_x = width / 2
        center_y = height / 2
        dist = np.sqrt((xx - center_x) ** 2 + (yy - center_y) ** 2)
        dist = dist / max(np.sqrt(center_x**2 + center_y**2), 1)
        damage_map = np.clip((1 - dist) * combined * 3.4, 0, 3)
        return breakdown, damage_map

    def _generate_heatmap(self, original: np.ndarray, damage_map: np.ndarray) -> str:
        height, width = original.shape[:2]
        damage_resized = np.array(
            Image.fromarray(np.uint8(np.clip(damage_map / 3.0, 0, 1) * 255)).resize((width, height))
        ).astype(np.float32)
        damage_scaled = np.clip((damage_resized / 255.0) * 3.0, 0, 3)

        color_map = np.zeros((height, width, 3), dtype=np.uint8)
        for class_idx, color in self.DAMAGE_COLORS.items():
            mask = (damage_scaled >= class_idx - 0.5) & (damage_scaled < class_idx + 0.5)
            color_map[mask] = color

        alpha = 0.45
        heatmap = (original.astype(np.float32) * (1 - alpha) + color_map.astype(np.float32) * alpha).clip(0, 255)
        image = Image.fromarray(heatmap.astype(np.uint8))
        self.heatmaps_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{uuid.uuid4().hex}.jpg"
        output_path = self.heatmaps_dir / filename
        image.save(output_path, format="JPEG", quality=88)
        return f"/{settings.heatmaps_dir.rstrip('/')}/{filename}"

    @staticmethod
    def _prepare_image(image: Image.Image, *, max_dim: int) -> Image.Image:
        width, height = image.size
        longest_side = max(width, height)
        if longest_side <= max_dim:
            return image
        scale = max_dim / float(longest_side)
        resized = image.resize((max(1, int(width * scale)), max(1, int(height * scale))), Image.Resampling.BILINEAR)
        return resized

    def _normalize_construction_type(self, value: str | None) -> str:
        normalized = (value or "").strip().upper().replace("É", "E").replace("È", "E").replace("-", " ")
        normalized = " ".join(normalized.split())
        return CONSTRUCTION_ALIASES.get(normalized, value.strip() if value else "Beton arme")

    def _normalize_zone(self, value: str | None) -> str:
        normalized = (value or "").strip()
        if normalized in {"0", "I", "IIa", "IIb", "III"}:
            return normalized
        normalized_lower = normalized.lower()
        mapping = {"iia": "IIa", "iib": "IIb", "iii": "III", "i": "I", "0": "0"}
        return mapping.get(normalized_lower, "IIa")

    @staticmethod
    def _avg_value_per_km2(construction_type: str) -> float:
        return {
            "Maconnerie creuse": 150_000_000.0,
            "Maconnerie chainee": 210_000_000.0,
            "Beton arme": 350_000_000.0,
            "Structure metallique": 500_000_000.0,
        }.get(construction_type, 220_000_000.0)


damage_ai_service = DamageAIService()
