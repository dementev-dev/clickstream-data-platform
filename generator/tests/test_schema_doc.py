"""Проверки «описания выгрузки»: свежесть документа и полнота таблицы.

Документ собирается из контракта, значит расходиться они могут только одним
способом — контракт правили, документ не пересобрали. Ровно это здесь и
сторожится.
"""

import re
from pathlib import Path

import pytest

from clickstream_generator.schema import COLUMNS, Column, ColumnGroup
from clickstream_generator.schema_doc import render

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC_PATH = REPO_ROOT / "docs" / "formats" / "clickstream-event.md"

TABLE_ROW = re.compile(r"^\| \d+ \|", re.MULTILINE)


@pytest.fixture(scope="module")
def rendered() -> str:
    return render()


def test_doc_is_up_to_date(rendered: str):
    assert DOC_PATH.exists(), f"описание выгрузки не найдено: {DOC_PATH}"
    assert DOC_PATH.read_text(encoding="utf-8") == rendered, (
        "описание выгрузки отстало от контракта — пересоберите: make docs"
    )


def test_every_column_has_a_row(rendered: str):
    assert len(TABLE_ROW.findall(rendered)) == len(COLUMNS)


def test_rows_are_numbered_in_contract_order(rendered: str):
    numbers = [int(row.strip("| ")) for row in TABLE_ROW.findall(rendered)]
    assert numbers == list(range(1, len(COLUMNS) + 1))


@pytest.mark.parametrize("column", COLUMNS, ids=lambda column: column.name)
def test_column_is_described_in_full(column: Column, rendered: str):
    """Колонку описывает одна строка, и в ней всё, что несёт контракт."""
    described = (
        column.name,
        column.clickhouse_type,
        column.numpy_dtype,
        column.dds_name,
        column.comment,
    )
    assert any(
        all(value in line for value in described) for line in rendered.splitlines()
    )


@pytest.mark.parametrize("group", list(ColumnGroup), ids=lambda group: group.name)
def test_group_is_a_heading(group: ColumnGroup, rendered: str):
    assert f"\n## {group.value}\n" in rendered
