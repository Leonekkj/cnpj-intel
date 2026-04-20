import json
import shutil
from pathlib import Path
from datetime import datetime, timezone

BASELINES_DIR = Path("tests/visual/baselines")
META_FILE = BASELINES_DIR / "baseline_meta.json"

THRESHOLDS = {
    "null_spike": 0.10,   # queda de fill rate que dispara anomalia
    "visual_diff": 0.05,  # % de pixels diferentes que dispara anomalia
}


def save_data_baseline(snapshot: dict) -> None:
    BASELINES_DIR.mkdir(parents=True, exist_ok=True)
    (BASELINES_DIR / "dashboard.json").write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    _update_meta()


def load_data_baseline() -> dict | None:
    p = BASELINES_DIR / "dashboard.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def save_screenshot_baseline(src_path: str, name: str) -> None:
    """Copia screenshot para baselines dir com nome padronizado."""
    BASELINES_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy(src_path, BASELINES_DIR / f"{name}.png")


def load_screenshot_baseline(name: str) -> Path | None:
    p = BASELINES_DIR / f"{name}.png"
    return p if p.exists() else None


def _update_meta() -> None:
    meta = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "thresholds": THRESHOLDS,
    }
    META_FILE.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def baselines_exist() -> bool:
    return (BASELINES_DIR / "dashboard.json").exists()
