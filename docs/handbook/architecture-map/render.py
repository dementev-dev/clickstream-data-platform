# /// script
# requires-python = ">=3.13"
# dependencies = ["pyyaml"]
# ///

"""Собрать интерактивную карту хранилища из YAML и HTML-шаблона."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).parent
SOURCE = ROOT / "map.yaml"
TEMPLATE = ROOT / "template.html"
OUTPUT = ROOT.parent / "architecture-map.html"
MARKER = "/*__MAP_DATA__*/"
NOTICE = (
    "<!-- Файл собран автоматически. Правьте architecture-map/map.yaml и "
    "template.html, затем выполните uv run "
    "docs/handbook/architecture-map/render.py. -->\n"
)


def render() -> str:
    """Подставить данные карты в шаблон без изменения порядка ключей."""
    data = yaml.safe_load(SOURCE.read_text(encoding="utf-8"))
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    template = TEMPLATE.read_text(encoding="utf-8")
    return NOTICE + template.replace(MARKER, payload)


def main() -> int:
    """Записать карту или проверить, что готовый файл не устарел."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="проверить готовый файл")
    args = parser.parse_args()
    rendered = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != rendered:
            print(f"{OUTPUT} устарел; пересоберите карту")
            return 1
        print(f"{OUTPUT} соответствует исходникам")
        return 0
    OUTPUT.write_text(rendered, encoding="utf-8")
    print(f"собрана карта: {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
