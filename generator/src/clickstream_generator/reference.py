"""Справочники мира: устройства, города, источники трафика, карта сайта.

Таблицы-литералы, а не посточный фейкер (спека генератора, разделы 6 и 9):
нам нужны не случайные строки, а связки — телефон тянет за собой Safari, iOS
и размер экрана; город тянет id региона и часовой пояс. Фейкер связок не
даёт, таблицу пришлось бы написать всё равно, а новая зависимость молча
меняла бы мир при обновлении своих словарей.

Веса долей живут в самих строках: доля неотделима от строки, которой она
принадлежит. Числа, ни к какой строке не привязанные — суточная волна, длина
визита, воронка, — лежат в `world.py`.

Настоящее здесь одно — гео-идентификаторы (оговорка в `City`). Магазин,
сайты-рефереры и адреса вымышлены: домен `example.com` отведён под примеры
RFC 2606, адреса нероутируемы.

Таблицы записаны руками (`fmt: off`): строка справочника — строка таблицы,
а не десять строк исходника.
"""

from dataclasses import dataclass
from enum import IntEnum
from urllib.parse import quote

# Магазин стенда: вымышленный, домен из зарезервированных под примеры.
SITE_NAME = "Дом и уют"
SITE_URL = "https://shop.example.com"


class Page(IntEnum):
    """Страницы магазина: пять для блуждания, три для воронки заказа."""

    HOME = 0
    CATALOG = 1
    SEARCH = 2
    PRODUCT = 3
    STATIC = 4
    CART = 5
    CHECKOUT = 6
    CONFIRMATION = 7


# Страницы блуждания: их порядок — порядок колонок в таблицах переходов ниже.
BROWSE_PAGES = (Page.HOME, Page.CATALOG, Page.SEARCH, Page.PRODUCT, Page.STATIC)

# Хвост воронки по порядку прохождения: сколько шагов пройдено — то и есть
# «докуда дошёл визит».
FUNNEL_PAGES = (Page.CART, Page.CHECKOUT, Page.CONFIRMATION)

# Куда посетитель уходит со страницы, проценты по колонкам `BROWSE_PAGES`.
# Правил всего два: с карточки товара идут смотреть соседние карточки и
# каталог, с любой листающей страницы — в карточку.
FROM_PRODUCT_PERCENT = (9, 30, 8, 45, 8)
FROM_LISTING_PERCENT = (6, 12, 6, 70, 6)

# Статические страницы: адрес и заголовок.
STATIC_PAGES = (
    ("/delivery", "Доставка и оплата"),
    ("/about", "О магазине"),
    ("/contacts", "Контакты"),
)

# Запросы к поиску по сайту. В адресе они лежат процентными кодами — так их
# отдаёт и выгрузка, и разбор такого адреса сам по себе материал лабы.
SEARCH_QUERIES = tuple(
    quote(text)
    for text in (
        "полотенца",
        "сковорода",
        "постельное бельё",
        "детский стульчик",
        "электрочайник",
        "шторы",
        "набор кастрюль",
        "тапочки",
    )
)


# Коды типа устройства у Метрики: 1 — десктоп, 2 — телефон, 3 — планшет,
# 4 — телевизор. Телефон назван отдельно: по нему различаются и мобильный
# адрес, и вторая кука пары — «телефон и ноутбук» (мастер-спека, раздел 5).
PHONE_CATEGORY = 2


@dataclass(frozen=True, slots=True)
class DeviceProfile:
    """Устройство посетителя целиком: браузер, ОС, экран.

    Это паспорт куки: кука — браузер на устройстве, поэтому во всех её
    визитах профиль один и тот же (спека генератора, раздел 9).
    """

    weight: int
    category: int  # коды Метрики, см. `PHONE_CATEGORY`
    browser: str
    browser_major_version: int
    language: str
    operating_system: str
    operating_system_root: str
    phone_model: str
    screen_width: int
    screen_height: int


# Доли устройств — правдоподобная российская розница: мобильных около двух
# третей, десктоп треть, планшеты тонкой полосой.
# fmt: off
DEVICE_PROFILES = (
    DeviceProfile(14, 1, "Chrome", 131, "ru",
                  "Windows 10", "Windows", "", 1920, 1080),
    DeviceProfile(6, 1, "Chrome", 131, "ru",
                  "Windows 11", "Windows", "", 1366, 768),
    DeviceProfile(6, 1, "YandexBrowser", 24, "ru",
                  "Windows 10", "Windows", "", 1920, 1080),
    DeviceProfile(3, 1, "Safari", 17, "ru",
                  "macOS 14", "macOS", "", 1440, 900),
    DeviceProfile(2, 1, "Firefox", 133, "ru",
                  "Windows 10", "Windows", "", 1600, 900),
    DeviceProfile(1, 1, "Chrome", 131, "en",
                  "Ubuntu 24.04", "Linux", "", 1920, 1080),
    DeviceProfile(10, 2, "Safari", 17, "ru",
                  "iOS 17.4", "iOS", "iPhone 14", 390, 844),
    DeviceProfile(6, 2, "Safari", 16, "ru",
                  "iOS 16.6", "iOS", "iPhone 12", 390, 844),
    DeviceProfile(13, 2, "Chrome", 131, "ru",
                  "Android 14", "Android", "Galaxy A53", 412, 915),
    DeviceProfile(12, 2, "Chrome", 130, "ru",
                  "Android 13", "Android", "Redmi Note 12", 393, 873),
    DeviceProfile(9, 2, "YandexBrowser", 24, "ru",
                  "Android 13", "Android", "Honor X8", 360, 780),
    DeviceProfile(6, 2, "Samsung Internet", 26, "ru",
                  "Android 14", "Android", "Galaxy S23", 360, 780),
    DeviceProfile(5, 2, "Chrome", 131, "ru",
                  "Android 12", "Android", "Vivo Y21", 360, 800),
    DeviceProfile(4, 3, "Safari", 17, "ru",
                  "iPadOS 17.4", "iOS", "", 810, 1080),
    DeviceProfile(3, 3, "Chrome", 130, "ru",
                  "Android 13", "Android", "", 800, 1280),
)
# fmt: on


@dataclass(frozen=True, slots=True)
class City:
    """Город посетителя: id региона, часовой пояс, ломоть адресов.

    Гео-идентификаторы — настоящие числа геобазы Яндекса, а не выдуманные:
    каждый проверен 2026-08-02 обращением к живым сервисам Яндекса по этому
    же номеру (`yandex.ru/pogoda/<id>`, `yandex.ru/maps/225/russia/`) —
    страница открывает ожидаемое место. Оговорка честная: опубликованной
    таблицы геобазы найти не удалось, а что `RegionCityID` Метрики нумерует
    регионы той же геобазой — обоснованное допущение, а не подтверждённый
    источником факт. Часовые пояса — из Википедии; часы в России не
    переводят с 2014 года, поэтому смещение постоянное.

    Адреса нероутируемые (спека генератора, раздел 9): городу отводится
    ломоть документационных сетей RFC 5737 или benchmark-сети 198.18/15.
    Правдоподобные публичные адреса принадлежат живым организациям, и в
    учебных данных им не место.
    """

    weight: int
    name: str  # по-английски, как в выгрузке Метрики
    region_id: int
    timezone_minutes: int
    ip_prefix: str


COUNTRY_NAME = "RU"
COUNTRY_REGION_ID = 225

# Регион присутствия — Поволжье с центром в Самаре: свой миллионник, города
# своего региона и тонкий хвост остальной страны (спека генератора,
# раздел 9). «Топ городов России» дал бы магазину с одним складом карту,
# которой у него быть не может.
CITIES = (
    City(34, "Samara", 51, 240, "192.0.2."),
    City(12, "Tolyatti", 240, 240, "198.51.100."),
    City(5, "Syzran", 11139, 240, "203.0.113."),
    City(7, "Ulyanovsk", 195, 240, "198.18.0."),
    City(7, "Saratov", 194, 240, "198.18.1."),
    City(5, "Penza", 49, 180, "198.18.2."),
    City(5, "Kazan", 43, 180, "198.18.3."),
    City(4, "Ufa", 172, 300, "198.18.4."),
    City(4, "Orenburg", 48, 300, "198.18.5."),
    City(3, "Nizhny Novgorod", 47, 180, "198.18.6."),
    City(2, "Volgograd", 38, 180, "198.18.7."),
    City(5, "Moscow", 213, 180, "198.18.8."),
    City(3, "Saint Petersburg", 2, 180, "198.18.9."),
    City(2, "Yekaterinburg", 54, 300, "198.18.10."),
    City(1, "Novosibirsk", 65, 420, "198.18.11."),
    City(1, "Krasnodar", 35, 180, "198.18.12."),
)

# Мобильный интернет: операторы раздают телефонам адреса CGNAT-диапазона
# 100.64/10 — второй байт от 64 до 127. Город по такому адресу не читается,
# как и в жизни.
MOBILE_IP_FIRST_BYTE = 100
MOBILE_IP_SECOND_BYTE = (64, 128)


@dataclass(frozen=True, slots=True)
class TrafficSource:
    """Откуда посетитель пришёл: реферер, метки перехода, вход на сайт.

    Метки кликов отданы колонками `HasGCLID` и `YCLID`: их выгрузка
    разбирает за нас. UTM остаются и в адресе входа — как в жизни.
    """

    weight: int
    last_traffic_source: str
    referer: str
    utm_source: str
    utm_medium: str
    utm_campaign: str
    utm_content: str
    utm_term: str
    has_gclid: int
    has_yclid: bool
    entry_percent: tuple[int, ...]  # вход на сайт по колонкам `BROWSE_PAGES`


# fmt: off
TRAFFIC_SOURCES = (
    TrafficSource(30, "organic", "https://yandex.ru/search/",
                  "", "", "", "", "", 0, False, (10, 30, 5, 50, 5)),
    TrafficSource(9, "organic", "https://www.google.com/",
                  "", "", "", "", "", 0, False, (10, 30, 5, 50, 5)),
    TrafficSource(22, "direct", "",
                  "", "", "", "", "", 0, False, (55, 15, 5, 15, 10)),
    TrafficSource(8, "ad", "https://yandex.ru/",
                  "yandex", "cpc", "posuda-poisk", "text-1", "kupit-skovorodu",
                  0, True, (5, 45, 0, 50, 0)),
    TrafficSource(6, "ad", "https://an.yandex.ru/",
                  "yandex", "cpc", "tekstil-rsya", "banner-2", "",
                  0, True, (5, 45, 0, 50, 0)),
    TrafficSource(5, "ad", "https://www.google.com/",
                  "google", "cpc", "home-shopping", "ad-1", "postelnoe-belyo",
                  1, False, (5, 45, 0, 50, 0)),
    TrafficSource(7, "social", "https://vk.com/",
                  "vk", "social", "vk-lenta", "post-3", "",
                  0, False, (15, 25, 0, 55, 5)),
    TrafficSource(5, "email", "",
                  "email", "email", "nedelnaya-rassylka", "blok-1", "",
                  0, False, (10, 40, 0, 45, 5)),
    TrafficSource(5, "referral", "https://market.example.com/",
                  "", "", "", "", "", 0, False, (5, 25, 0, 65, 5)),
    TrafficSource(3, "recommend", "https://dzen.ru/",
                  "", "", "", "", "", 0, False, (10, 30, 0, 55, 5)),
)
# fmt: on
