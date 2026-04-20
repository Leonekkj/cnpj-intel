#!/usr/bin/env python3
"""
Audit noturno do CNPJ Intel dashboard.

Uso:
  python scripts/dashboard_audit.py                  # run normal
  python scripts/dashboard_audit.py --update-baselines  # atualizar referências
"""
import sys
import argparse
import tempfile
from pathlib import Path

# Garante que o root do projeto está no path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.audit.capture import collect_data_snapshot, capture_screenshots
from scripts.audit.baselines import (
    baselines_exist,
    save_data_baseline,
    save_screenshot_baseline,
    load_data_baseline,
)
from scripts.audit.detector import detect_all
from scripts.audit.reporter import report


def run_update_baselines() -> None:
    print("[audit] Modo --update-baselines: capturando referências...")
    snapshot = collect_data_snapshot()
    save_data_baseline(snapshot)
    print(f"[audit] Snapshot salvo: {len(snapshot.get('cnaes', []))} CNAEs, "
          f"fill_rates={snapshot.get('fill_rates')}")

    with tempfile.TemporaryDirectory() as tmpdir:
        screenshots = capture_screenshots(tmpdir)
        for name, path in screenshots.items():
            save_screenshot_baseline(path, name)
            print(f"[audit] Screenshot baseline salvo: {name}")

    print("[audit] Baselines atualizados com sucesso.")


def run_audit() -> int:
    """Retorna 0 se OK, 1 se anomalias encontradas."""
    if not baselines_exist():
        print("[audit] ERRO: baselines não encontrados. "
              "Execute com --update-baselines primeiro.")
        return 1

    print("[audit] Capturando snapshot atual...")
    baseline = load_data_baseline()
    snapshot = collect_data_snapshot()

    with tempfile.TemporaryDirectory() as tmpdir:
        screenshots = capture_screenshots(tmpdir)
        anomalies = detect_all(snapshot, baseline, screenshots)

    if not anomalies:
        print("[audit] Nenhuma anomalia detectada. Dashboard OK.")
        return 0

    print(f"[audit] {len(anomalies)} anomalia(s) detectada(s):")
    for a in anomalies:
        print(f"  [{a.severity.upper()}] {a.type}: {a.description}")

    report(anomalies)
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="CNPJ Intel dashboard audit")
    parser.add_argument("--update-baselines", action="store_true",
                        help="Atualiza arquivos de referência em vez de auditar")
    args = parser.parse_args()

    if args.update_baselines:
        run_update_baselines()
        sys.exit(0)
    else:
        sys.exit(run_audit())


if __name__ == "__main__":
    main()
