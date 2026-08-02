"""Сторожа справочников: связки, из-за которых таблицы написаны руками.

Тесты держат не содержание строк — его менти волен менять, — а то, на чём
стоит день-функция: доли складываются в сотню, вход и переходы разложены по
страницам блуждания, устройство не противоречит само себе.
"""

from clickstream_generator import reference


def test_shares_of_every_directory_add_up_to_a_hundred():
    """Веса — проценты: выбор по ним целочисленный и без остатка."""
    for table in (
        reference.DEVICE_PROFILES,
        reference.CITIES,
        reference.TRAFFIC_SOURCES,
    ):
        assert sum(row.weight for row in table) == 100


def test_walking_the_site_never_leaves_the_browsing_pages():
    for percent in (reference.FROM_PRODUCT_PERCENT, reference.FROM_LISTING_PERCENT):
        assert len(percent) == len(reference.BROWSE_PAGES)
        assert sum(percent) == 100


def test_a_visitor_comes_to_a_page_that_exists():
    for source in reference.TRAFFIC_SOURCES:
        assert len(source.entry_percent) == len(reference.BROWSE_PAGES)
        assert sum(source.entry_percent) == 100


def test_the_walk_prefers_the_product_card():
    """Иначе магазин выглядел бы каталогом, который никто не открывает."""
    card = reference.BROWSE_PAGES.index(reference.Page.PRODUCT)
    assert reference.FROM_LISTING_PERCENT[card] > 50


def test_a_phone_carries_a_model_and_a_desktop_does_not():
    for profile in reference.DEVICE_PROFILES:
        phone = profile.category == reference.PHONE_CATEGORY
        assert bool(profile.phone_model) == phone
        assert profile.screen_width > 0 and profile.screen_height > 0


def test_mobile_traffic_outweighs_the_desktop():
    """Российская розница мобильная; на этом стоят доли устройств."""
    phones = sum(
        row.weight
        for row in reference.DEVICE_PROFILES
        if row.category == reference.PHONE_CATEGORY
    )
    assert phones > 50


def test_cities_are_told_apart_by_id_and_by_address_block():
    assert len({city.region_id for city in reference.CITIES}) == len(reference.CITIES)
    assert len({city.ip_prefix for city in reference.CITIES}) == len(reference.CITIES)
    assert reference.COUNTRY_REGION_ID not in {c.region_id for c in reference.CITIES}


def test_the_region_of_presence_outweighs_the_rest_of_the_country():
    """Один регион присутствия, а не «топ городов России» (спека, раздел 9)."""
    home = reference.CITIES[0]
    assert home.name == "Samara"
    assert home.weight > 30
    nearby = sum(
        city.weight
        for city in reference.CITIES
        if city.timezone_minutes == home.timezone_minutes
    )
    assert nearby > 50


def test_timezones_are_whole_hours():
    """Волна поворачивается на целые часы: получасовых поясов в мире нет."""
    for city in reference.CITIES:
        assert city.timezone_minutes % 60 == 0


def test_paid_sources_carry_their_click_labels():
    for source in reference.TRAFFIC_SOURCES:
        paid = source.last_traffic_source == "ad"
        assert bool(source.utm_source) >= paid
        assert (source.has_gclid or source.has_yclid) == paid


def test_free_sources_carry_no_utm():
    """Метки ставит тот, кто платит: у organic и direct их не бывает."""
    for source in reference.TRAFFIC_SOURCES:
        if source.last_traffic_source in ("organic", "direct", "recommend"):
            assert not source.utm_source
