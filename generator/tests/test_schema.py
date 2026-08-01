"""Инварианты контракта схемы события.

Контракт — чистые данные, поэтому проверять в нём нечего кроме связности:
состав, уникальность имён, заполненность полей, согласие типов и порядок.
Это и есть сторож границы «трекер | хранилище»: молчаливый дрейф колонок
ловится здесь, а не в DDL через неделю.
"""

import re

import numpy as np
import pytest

from clickstream_generator.schema import COLUMNS, Column, ColumnGroup

# Состав решён мастер-спекой (раздел 1.2) и в этом тикете не переоткрывается.
EXPECTED_COLUMN_COUNT = 47

# Тот же состав, переписанный с мастер-спеки отдельно от контракта: группа,
# имя, тип. Дубль намеренный — только независимая запись ловит молчаливое
# переименование колонки, подмену типа или перестановку. Правка контракта без
# правки спеки краснеет здесь, и это единственный способ узнать о ней вовремя.
MASTER_SPEC_COMPOSITION = (
    (ColumnGroup.IDENTIFIERS, "WatchID", "UInt64"),
    (ColumnGroup.IDENTIFIERS, "VisitID", "UInt64"),
    (ColumnGroup.IDENTIFIERS, "ClientID", "UInt64"),
    (ColumnGroup.IDENTIFIERS, "CounterID", "UInt32"),
    (ColumnGroup.IDENTIFIERS, "EventDate", "Date"),
    (ColumnGroup.IDENTIFIERS, "UTCEventTime", "DateTime"),
    (ColumnGroup.IDENTIFIERS, "ClientTimeZone", "Int16"),
    (ColumnGroup.IDENTIFIERS, "EventType", "LowCardinality(String)"),
    (ColumnGroup.IDENTIFIERS, "Sign", "Int8"),
    (ColumnGroup.PAGE, "URL", "String"),
    (ColumnGroup.PAGE, "Referer", "String"),
    (ColumnGroup.PAGE, "Title", "String"),
    (ColumnGroup.PAGE, "UTMSource", "String"),
    (ColumnGroup.PAGE, "UTMMedium", "String"),
    (ColumnGroup.PAGE, "UTMCampaign", "String"),
    (ColumnGroup.PAGE, "UTMContent", "String"),
    (ColumnGroup.PAGE, "UTMTerm", "String"),
    (ColumnGroup.PAGE, "LastTrafficSource", "String"),
    (ColumnGroup.PAGE, "HasGCLID", "UInt8"),
    (ColumnGroup.PAGE, "YCLID", "UInt64"),
    (ColumnGroup.CLIENT, "Browser", "String"),
    (ColumnGroup.CLIENT, "BrowserMajorVersion", "UInt16"),
    (ColumnGroup.CLIENT, "BrowserLanguage", "String"),
    (ColumnGroup.CLIENT, "OperatingSystem", "String"),
    (ColumnGroup.CLIENT, "OperatingSystemRoot", "String"),
    (ColumnGroup.CLIENT, "DeviceCategory", "UInt8"),
    (ColumnGroup.CLIENT, "MobilePhoneModel", "String"),
    (ColumnGroup.CLIENT, "ScreenWidth", "UInt16"),
    (ColumnGroup.CLIENT, "ScreenHeight", "UInt16"),
    (ColumnGroup.CLIENT, "IPAddress", "String"),
    (ColumnGroup.CLIENT, "RegionCountry", "String"),
    (ColumnGroup.CLIENT, "RegionCity", "String"),
    (ColumnGroup.CLIENT, "RegionCountryID", "UInt32"),
    (ColumnGroup.CLIENT, "RegionCityID", "UInt32"),
    (ColumnGroup.PARAMS, "GoalsReached", "Array(UInt32)"),
    (ColumnGroup.PARAMS, "ParsedParamsKey1", "Array(String)"),
    (ColumnGroup.ECOMMERCE, "purchaseID", "Array(String)"),
    (ColumnGroup.ECOMMERCE, "purchaseRevenue", "Array(Float64)"),
    (ColumnGroup.ECOMMERCE, "purchaseCurrency", "Array(String)"),
    (ColumnGroup.ECOMMERCE, "purchaseCoupon", "Array(String)"),
    (ColumnGroup.ECOMMERCE, "productID", "Array(String)"),
    (ColumnGroup.ECOMMERCE, "productName", "Array(String)"),
    (ColumnGroup.ECOMMERCE, "productCategory", "Array(String)"),
    (ColumnGroup.ECOMMERCE, "productPrice", "Array(Int64)"),
    (ColumnGroup.ECOMMERCE, "productQuantity", "Array(UInt64)"),
    (ColumnGroup.ECOMMERCE, "productEventType", "Array(String)"),
    (ColumnGroup.ECOMMERCE, "ecommerce", "String"),
)

# Соответствие «тип ClickHouse — тип numpy», записанное независимо от
# контракта: если пара в контракте разъедется, сойтись они уже не смогут.
NUMPY_BY_CLICKHOUSE_TYPE = {
    "UInt8": "uint8",
    "UInt16": "uint16",
    "UInt32": "uint32",
    "UInt64": "uint64",
    "Int8": "int8",
    "Int16": "int16",
    "Int64": "int64",
    "Float64": "float64",
    "String": "object",
    "LowCardinality(String)": "object",
    "Date": "datetime64[D]",
    "DateTime": "datetime64[s]",
}

# Имена для DDS — не производная от имён Метрики, а решение тикета #36:
# вывести их правилом нельзя (акронимы, «timezone» одним словом), поэтому
# сверять их не с чем, кроме такой же независимой записи. Без неё осмысленно
# неверное имя молча уезжает в опубликованное описание выгрузки.
EXPECTED_DDS_NAMES = {
    "WatchID": "watch_id",
    "VisitID": "visit_id",
    "ClientID": "client_id",
    "CounterID": "counter_id",
    "EventDate": "event_date",
    "UTCEventTime": "utc_event_time",
    "ClientTimeZone": "client_timezone",
    "EventType": "event_type",
    "Sign": "sign",
    "URL": "url",
    "Referer": "referer",
    "Title": "title",
    "UTMSource": "utm_source",
    "UTMMedium": "utm_medium",
    "UTMCampaign": "utm_campaign",
    "UTMContent": "utm_content",
    "UTMTerm": "utm_term",
    "LastTrafficSource": "last_traffic_source",
    "HasGCLID": "has_gclid",
    "YCLID": "yclid",
    "Browser": "browser",
    "BrowserMajorVersion": "browser_major_version",
    "BrowserLanguage": "browser_language",
    "OperatingSystem": "operating_system",
    "OperatingSystemRoot": "operating_system_root",
    "DeviceCategory": "device_category",
    "MobilePhoneModel": "mobile_phone_model",
    "ScreenWidth": "screen_width",
    "ScreenHeight": "screen_height",
    "IPAddress": "ip_address",
    "RegionCountry": "region_country",
    "RegionCity": "region_city",
    "RegionCountryID": "region_country_id",
    "RegionCityID": "region_city_id",
    "GoalsReached": "goals_reached",
    "ParsedParamsKey1": "parsed_params_key1",
    "purchaseID": "purchase_id",
    "purchaseRevenue": "purchase_revenue",
    "purchaseCurrency": "purchase_currency",
    "purchaseCoupon": "purchase_coupon",
    "productID": "product_id",
    "productName": "product_name",
    "productCategory": "product_category",
    "productPrice": "product_price",
    "productQuantity": "product_quantity",
    "productEventType": "product_event_type",
    "ecommerce": "ecommerce",
}

METRICA_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9]*$")
DDS_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
ARRAY_TYPE = re.compile(r"^Array\((.+)\)$")


def element_type(clickhouse_type: str) -> str:
    """Тип элемента: у массива — то, что внутри `Array(...)`, иначе сам тип."""
    array = ARRAY_TYPE.match(clickhouse_type)
    return array.group(1) if array else clickhouse_type


def test_columns_are_an_immutable_sequence():
    assert isinstance(COLUMNS, tuple)


def test_column_count():
    assert len(COLUMNS) == EXPECTED_COLUMN_COUNT


def test_composition_matches_master_spec():
    """Состав, имена, типы и порядок — те же, что в разделе 1.2 мастер-спеки."""
    composition = tuple(
        (column.group, column.name, column.clickhouse_type) for column in COLUMNS
    )
    assert composition == MASTER_SPEC_COMPOSITION


def test_metrica_names_are_unique():
    names = [column.name for column in COLUMNS]
    assert len(set(names)) == len(names)


def test_dds_names_are_unique():
    names = [column.dds_name for column in COLUMNS]
    assert len(set(names)) == len(names)


def test_dds_names_are_the_ones_we_chose():
    """Переименование колонки в DDS — решение, а не правка мимоходом."""
    assert {column.name: column.dds_name for column in COLUMNS} == EXPECTED_DDS_NAMES


@pytest.mark.parametrize("column", COLUMNS, ids=lambda column: column.name)
def test_attributes_are_filled(column: Column):
    assert column.name.strip()
    assert column.clickhouse_type.strip()
    assert column.numpy_dtype.strip()
    assert column.dds_name.strip()
    assert column.comment.strip()
    assert isinstance(column.group, ColumnGroup)


@pytest.mark.parametrize("column", COLUMNS, ids=lambda column: column.name)
def test_names_keep_their_styles(column: Column):
    assert METRICA_NAME.match(column.name), "имя источника — как в выгрузке Метрики"
    assert DDS_NAME.match(column.dds_name), "имя для DDS — snake_case"


@pytest.mark.parametrize("column", COLUMNS, ids=lambda column: column.name)
def test_numpy_dtype_exists(column: Column):
    assert np.dtype(column.numpy_dtype).name == column.numpy_dtype


@pytest.mark.parametrize("column", COLUMNS, ids=lambda column: column.name)
def test_numpy_dtype_matches_clickhouse_type(column: Column):
    expected = NUMPY_BY_CLICKHOUSE_TYPE.get(element_type(column.clickhouse_type))
    assert expected is not None, f"незнакомый тип ClickHouse: {column.clickhouse_type}"
    assert column.numpy_dtype == expected


def test_groups_go_in_runs_and_in_order():
    """Группы не чередуются: каждая идёт одним куском, куски — по объявлению."""
    seen = []
    for column in COLUMNS:
        if not seen or seen[-1] is not column.group:
            assert column.group not in seen, f"группа {column.group.name} разорвана"
            seen.append(column.group)
    assert seen == list(ColumnGroup), "порядок групп разошёлся с их объявлением"
