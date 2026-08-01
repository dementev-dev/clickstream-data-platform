"""Контракт схемы события: чистые данные о колонках выгрузки, никакой логики.

Состав, имена и типы решены мастер-спекой «Боевой реализм стенда» (разделы
1.1–1.2) и здесь не переоткрываются — модуль записывает их машинно-читаемо.
Контракт принадлежит генератору и кормит трёх потребителей: сам генератор,
его валидацию и «описание выгрузки» в доках (`schema_doc`). Хранилище
строится по описанию, а не по модулю; границу сторожит contract-тест,
сверяющий `system.columns` поднятого стенда с этим контрактом.

Что несёт описатель колонки:

- `name` — имя источника, как в облачной выгрузке Метрики; сырой слой хранит
  его без изменений.
- `clickhouse_type` — тип в хранилище ровно в той записи, в какой его вернёт
  `system.columns`.
- `numpy_dtype` — чем колонка представлена внутри генератора; у массивов это
  тип элемента. Строки живут в `object`-массивах: numpy-строки фиксированной
  длины стенду ничего не дают.
- `dds_name` — наше snake_case-имя, под которым колонка появится в DDS.
  Перевод механический, акроним идёт одним куском (`utm_source`, `has_gclid`,
  `ip_address`); единственное исключение — `client_timezone`: «timezone»
  пишем одним словом.
- `group` — раздел описания выгрузки; колонки одной группы идут подряд.
- `comment` — строка описания для менти, попадает в документ как есть.

Порядок колонок несёт сам кортеж `COLUMNS` — он и есть порядок выгрузки, по
нему считается нумерация в документе. Отдельного поля с номером намеренно
нет: два источника порядка разъезжаются при первой же вставке колонки
в середину.
"""

from dataclasses import dataclass
from enum import Enum


class ColumnGroup(Enum):
    """Разделы описания выгрузки; порядок объявления — порядок в документе."""

    IDENTIFIERS = "Идентификаторы и время"
    PAGE = "Страница и атрибуция"
    CLIENT = "Браузер, устройство, гео"
    PARAMS = "Массивы и параметры"
    ECOMMERCE = "Ecommerce"


@dataclass(frozen=True, slots=True)
class Column:
    """Описатель одной колонки выгрузки."""

    name: str
    clickhouse_type: str
    numpy_dtype: str
    dds_name: str
    group: ColumnGroup
    comment: str


COLUMNS: tuple[Column, ...] = (
    Column(
        name="WatchID",
        clickhouse_type="UInt64",
        numpy_dtype="uint64",
        dds_name="watch_id",
        group=ColumnGroup.IDENTIFIERS,
        comment="id события — хита; держится ниже 2^53, выше числа в JSON округляются",
    ),
    Column(
        name="VisitID",
        clickhouse_type="UInt64",
        numpy_dtype="uint64",
        dds_name="visit_id",
        group=ColumnGroup.IDENTIFIERS,
        comment="id визита от генератора — эталон лабы: собери сессии сам и сравни",
    ),
    Column(
        name="ClientID",
        clickhouse_type="UInt64",
        numpy_dtype="uint64",
        dds_name="client_id",
        group=ColumnGroup.IDENTIFIERS,
        comment="анонимный id браузера — кука; по хешу от неё таблица шардируется",
    ),
    Column(
        name="CounterID",
        clickhouse_type="UInt32",
        numpy_dtype="uint32",
        dds_name="counter_id",
        group=ColumnGroup.IDENTIFIERS,
        comment="id счётчика: на стенде константа, сайт один",
    ),
    Column(
        name="EventDate",
        clickhouse_type="Date",
        numpy_dtype="datetime64[D]",
        dds_name="event_date",
        group=ColumnGroup.IDENTIFIERS,
        comment="дата события; по ней режется партиция",
    ),
    Column(
        name="UTCEventTime",
        clickhouse_type="DateTime",
        numpy_dtype="datetime64[s]",
        dds_name="utc_event_time",
        group=ColumnGroup.IDENTIFIERS,
        comment="время события в UTC — единственная метка времени, как у Метрики",
    ),
    Column(
        name="ClientTimeZone",
        clickhouse_type="Int16",
        numpy_dtype="int16",
        dds_name="client_timezone",
        group=ColumnGroup.IDENTIFIERS,
        comment="смещение часового пояса клиента от UTC, в минутах",
    ),
    Column(
        name="EventType",
        clickhouse_type="LowCardinality(String)",
        numpy_dtype="object",
        dds_name="event_type",
        group=ColumnGroup.IDENTIFIERS,
        comment="тип события: pageview, add_to_cart, purchase — добавка стенда,"
        " у Метрики такого поля нет",
    ),
    Column(
        name="Sign",
        clickhouse_type="Int8",
        numpy_dtype="int8",
        dds_name="sign",
        group=ColumnGroup.IDENTIFIERS,
        comment="всегда 1: колонка формата, исправлений записей генератор не шлёт",
    ),
    Column(
        name="URL",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="url",
        group=ColumnGroup.PAGE,
        comment="адрес страницы события",
    ),
    Column(
        name="Referer",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="referer",
        group=ColumnGroup.PAGE,
        comment="адрес, с которого посетитель пришёл на страницу",
    ),
    Column(
        name="Title",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="title",
        group=ColumnGroup.PAGE,
        comment="заголовок страницы",
    ),
    Column(
        name="UTMSource",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="utm_source",
        group=ColumnGroup.PAGE,
        comment="метка utm_source: площадка перехода",
    ),
    Column(
        name="UTMMedium",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="utm_medium",
        group=ColumnGroup.PAGE,
        comment="метка utm_medium: тип трафика",
    ),
    Column(
        name="UTMCampaign",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="utm_campaign",
        group=ColumnGroup.PAGE,
        comment="метка utm_campaign: рекламная кампания",
    ),
    Column(
        name="UTMContent",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="utm_content",
        group=ColumnGroup.PAGE,
        comment="метка utm_content: что различает объявления одной кампании",
    ),
    Column(
        name="UTMTerm",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="utm_term",
        group=ColumnGroup.PAGE,
        comment="метка utm_term: ключевое слово перехода",
    ),
    Column(
        name="LastTrafficSource",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="last_traffic_source",
        group=ColumnGroup.PAGE,
        comment="последний источник трафика: direct, organic, ad, referral",
    ),
    Column(
        name="HasGCLID",
        clickhouse_type="UInt8",
        numpy_dtype="uint8",
        dds_name="has_gclid",
        group=ColumnGroup.PAGE,
        comment="1, если в адресе была метка Google Ads",
    ),
    Column(
        name="YCLID",
        clickhouse_type="UInt64",
        numpy_dtype="uint64",
        dds_name="yclid",
        group=ColumnGroup.PAGE,
        comment="идентификатор клика Яндекс Директа; 0 — метки не было",
    ),
    Column(
        name="Browser",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="browser",
        group=ColumnGroup.CLIENT,
        comment="браузер посетителя",
    ),
    Column(
        name="BrowserMajorVersion",
        clickhouse_type="UInt16",
        numpy_dtype="uint16",
        dds_name="browser_major_version",
        group=ColumnGroup.CLIENT,
        comment="старшая версия браузера",
    ),
    Column(
        name="BrowserLanguage",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="browser_language",
        group=ColumnGroup.CLIENT,
        comment="язык браузера",
    ),
    Column(
        name="OperatingSystem",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="operating_system",
        group=ColumnGroup.CLIENT,
        comment="операционная система с версией",
    ),
    Column(
        name="OperatingSystemRoot",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="operating_system_root",
        group=ColumnGroup.CLIENT,
        comment="семейство операционной системы, без версии",
    ),
    Column(
        name="DeviceCategory",
        clickhouse_type="UInt8",
        numpy_dtype="uint8",
        dds_name="device_category",
        group=ColumnGroup.CLIENT,
        comment="тип устройства кодами 1–4, как у Метрики; у неё это строка —"
        " отступление стенда",
    ),
    Column(
        name="MobilePhoneModel",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="mobile_phone_model",
        group=ColumnGroup.CLIENT,
        comment="модель телефона; на десктопе пусто",
    ),
    Column(
        name="ScreenWidth",
        clickhouse_type="UInt16",
        numpy_dtype="uint16",
        dds_name="screen_width",
        group=ColumnGroup.CLIENT,
        comment="ширина экрана в пикселях",
    ),
    Column(
        name="ScreenHeight",
        clickhouse_type="UInt16",
        numpy_dtype="uint16",
        dds_name="screen_height",
        group=ColumnGroup.CLIENT,
        comment="высота экрана в пикселях",
    ),
    Column(
        name="IPAddress",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="ip_address",
        group=ColumnGroup.CLIENT,
        comment="IP-адрес посетителя",
    ),
    Column(
        name="RegionCountry",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="region_country",
        group=ColumnGroup.CLIENT,
        comment="страна кодом ISO",
    ),
    Column(
        name="RegionCity",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="region_city",
        group=ColumnGroup.CLIENT,
        comment="город, название по-английски",
    ),
    Column(
        name="RegionCountryID",
        clickhouse_type="UInt32",
        numpy_dtype="uint32",
        dds_name="region_country_id",
        group=ColumnGroup.CLIENT,
        comment="числовой id страны в справочнике регионов Яндекса",
    ),
    Column(
        name="RegionCityID",
        clickhouse_type="UInt32",
        numpy_dtype="uint32",
        dds_name="region_city_id",
        group=ColumnGroup.CLIENT,
        comment="числовой id города в том же справочнике",
    ),
    Column(
        name="GoalsReached",
        clickhouse_type="Array(UInt32)",
        numpy_dtype="uint32",
        dds_name="goals_reached",
        group=ColumnGroup.PARAMS,
        comment="id достигнутых целей; на стенде их две — корзина и покупка",
    ),
    Column(
        name="ParsedParamsKey1",
        clickhouse_type="Array(String)",
        numpy_dtype="object",
        dds_name="parsed_params_key1",
        group=ColumnGroup.PARAMS,
        comment="свои параметры сайта, один уровень — например вариант A/B-теста",
    ),
    Column(
        name="purchaseID",
        clickhouse_type="Array(String)",
        numpy_dtype="object",
        dds_name="purchase_id",
        group=ColumnGroup.ECOMMERCE,
        comment="номер заказа; у события purchase — один элемент",
    ),
    Column(
        name="purchaseRevenue",
        clickhouse_type="Array(Float64)",
        numpy_dtype="float64",
        dds_name="purchase_revenue",
        group=ColumnGroup.ECOMMERCE,
        comment="выручка заказа глазами клиента; Float64, как у Метрики —"
        " на этом держится урок о расхождениях с бэкендом",
    ),
    Column(
        name="purchaseCurrency",
        clickhouse_type="Array(String)",
        numpy_dtype="object",
        dds_name="purchase_currency",
        group=ColumnGroup.ECOMMERCE,
        comment="валюта заказа",
    ),
    Column(
        name="purchaseCoupon",
        clickhouse_type="Array(String)",
        numpy_dtype="object",
        dds_name="purchase_coupon",
        group=ColumnGroup.ECOMMERCE,
        comment="купон заказа, если был применён",
    ),
    Column(
        name="productID",
        clickhouse_type="Array(String)",
        numpy_dtype="object",
        dds_name="product_id",
        group=ColumnGroup.ECOMMERCE,
        comment="id товаров события",
    ),
    Column(
        name="productName",
        clickhouse_type="Array(String)",
        numpy_dtype="object",
        dds_name="product_name",
        group=ColumnGroup.ECOMMERCE,
        comment="названия тех же товаров",
    ),
    Column(
        name="productCategory",
        clickhouse_type="Array(String)",
        numpy_dtype="object",
        dds_name="product_category",
        group=ColumnGroup.ECOMMERCE,
        comment="категории тех же товаров",
    ),
    Column(
        name="productPrice",
        clickhouse_type="Array(Int64)",
        numpy_dtype="int64",
        dds_name="product_price",
        group=ColumnGroup.ECOMMERCE,
        comment="цена за штуку целым числом: деньги генератор считает целыми",
    ),
    Column(
        name="productQuantity",
        clickhouse_type="Array(UInt64)",
        numpy_dtype="uint64",
        dds_name="product_quantity",
        group=ColumnGroup.ECOMMERCE,
        comment="количество штук каждого товара",
    ),
    Column(
        name="productEventType",
        clickhouse_type="Array(String)",
        numpy_dtype="object",
        dds_name="product_event_type",
        group=ColumnGroup.ECOMMERCE,
        comment="действие с товаром: detail, add, remove, purchase",
    ),
    Column(
        name="ecommerce",
        clickhouse_type="String",
        numpy_dtype="object",
        dds_name="ecommerce",
        group=ColumnGroup.ECOMMERCE,
        comment="сырой JSON события, как отдаёт Метрика — материал лабы"
        " про разбор JSON внутри колонки",
    ),
)
