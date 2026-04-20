from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from scripts.audit.baselines import THRESHOLDS, load_screenshot_baseline

try:
    from PIL import Image, ImageChops
    import numpy as np
    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False


@dataclass
class Anomaly:
    type: Literal["null_spike", "cnae_missing", "visual_diff"]
    severity: Literal["warning", "critical"]
    description: str
    evidence: dict  # dados brutos para o reporter


def detect_null_spikes(snapshot: dict, baseline: dict) -> list[Anomaly]:
    anomalies = []
    threshold = THRESHOLDS["null_spike"]
    current = snapshot.get("fill_rates", {})
    reference = baseline.get("fill_rates", {})
    for field, ref_rate in reference.items():
        curr_rate = current.get(field, ref_rate)
        drop = ref_rate - curr_rate
        if drop > threshold:
            anomalies.append(Anomaly(
                type="null_spike",
                severity="critical" if drop >= 0.19 else "warning",
                description=(
                    f"Campo '{field}': fill rate caiu de {ref_rate:.1%} para {curr_rate:.1%} "
                    f"(queda de {drop:.1%}, limiar: {threshold:.1%})"
                ),
                evidence={
                    "field": field,
                    "baseline_rate": ref_rate,
                    "current_rate": curr_rate,
                    "drop": drop,
                },
            ))
    return anomalies


def detect_cnae_gaps(snapshot: dict, baseline: dict) -> list[Anomaly]:
    anomalies = []
    baseline_cnaes = set(baseline.get("cnaes", []))
    current_cnaes = set(snapshot.get("cnaes", []))
    missing = baseline_cnaes - current_cnaes
    if missing:
        anomalies.append(Anomaly(
            type="cnae_missing",
            severity="warning",
            description=f"{len(missing)} categoria(s) CNAE ausente(s): {', '.join(sorted(missing)[:10])}",
            evidence={"missing_cnaes": sorted(missing)},
        ))
    return anomalies


def detect_visual_diff(page_name: str, current_screenshot_path: str) -> list[Anomaly]:
    if not HAS_PILLOW:
        return []
    baseline_path = load_screenshot_baseline(page_name)
    if baseline_path is None:
        return []
    try:
        img_base = Image.open(baseline_path).convert("RGB")
        img_curr = Image.open(current_screenshot_path).convert("RGB")
        if img_base.size != img_curr.size:
            img_curr = img_curr.resize(img_base.size)
        diff = ImageChops.difference(img_base, img_curr)
        import numpy as np
        arr = np.array(diff)
        diff_pct = float((arr > 10).any(axis=2).mean())
        threshold = THRESHOLDS["visual_diff"]
        if diff_pct > threshold:
            return [Anomaly(
                type="visual_diff",
                severity="warning",
                description=(
                    f"Página '{page_name}': {diff_pct:.1%} de pixels diferentes "
                    f"(limiar: {threshold:.1%})"
                ),
                evidence={
                    "page": page_name,
                    "diff_pct": diff_pct,
                    "current_screenshot": current_screenshot_path,
                    "baseline_screenshot": str(baseline_path),
                },
            )]
    except Exception:
        pass  # falha silenciosa em diff visual não deve parar o audit
    return []


def detect_all(snapshot: dict, baseline: dict, screenshots: dict[str, str]) -> list[Anomaly]:
    anomalies = []
    anomalies.extend(detect_null_spikes(snapshot, baseline))
    anomalies.extend(detect_cnae_gaps(snapshot, baseline))
    for page_name, path in screenshots.items():
        anomalies.extend(detect_visual_diff(page_name, path))
    return anomalies
