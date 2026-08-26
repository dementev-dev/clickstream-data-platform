-- DDS: физические модели слоя и доноры их партиций.
--
-- Идемпотентность здесь устроена иначе, чем в приёме, и это главный урок обеих
-- сущностей файла. В ODS повтор гасит колонка версии:
-- ReplacingMergeTree(updated_at) оставляет позднюю строку. Здесь колонки
-- версии нет вовсе — повтор гасит замена партиции целиком: день собирается
-- заново и подменяется одним атомарным движением. Поэтому строка таблицы
-- ровно одна на своё зерно, а чтению не нужен FINAL.

-- Локальная таблица заказа.
--
-- Зерно — заказ в текущем состоянии, ключ order_id. Историю версий слой не
-- хранит: у неё нет читателя ни в одной витрине, а ODS её и не обещает
-- (docs/architecture/dds/order.md, «Зерно»).
--
-- Партиция — order_date, день заказа в поясе счётчика. Партиция дневная не
-- ради размера куска, а потому что день — единица пересборки: пока заказ
-- внутри окна изменяемости, его день «дышит» и собирается заново, за окном
-- замерзает. Ключ партиции и есть контракт трансформации с хранилищем.
--
-- Ключ сортировки — order_id: единственный устойчивый ключ сущности, он же
-- ключ шардирования ниже. Колонки версии в движке нет: см. шапку файла.
CREATE TABLE IF NOT EXISTS dds.order_rep ON CLUSTER clickstream_cluster
(
    order_id String,
    user_id UInt64,
    status LowCardinality(String),
    -- День заказа посчитан один раз при загрузке, по образцу EventDate у
    -- события: суточные срезы группируются по готовой дате и пояса не
    -- упоминают (docs/architecture/storage.md, «Часовые пояса»).
    order_date Date,
    -- Времена лежат местными: DDS обслуживает человека с дашбордом, и пояс
    -- пересчитан один раз здесь, а не в каждом отчёте. Линза названа в типе,
    -- поэтому пояс сервера на эти колонки не влияет.
    created_at DateTime64(3, 'Europe/Samara'),
    updated_at DateTime64(3, 'Europe/Samara'),
    items_total Decimal(18, 2),
    discount Decimal(18, 2),
    delivery Decimal(18, 2),
    total Decimal(18, 2),
    -- Позиции — три массива одной длины: артикул, количество, цена. Родная
    -- для колоночного хранилища денормализация: позиция не живёт без заказа и
    -- своего ключа не имеет, а разворачивает массивы читатель — ARRAY JOIN в
    -- витрине выручки. Форма записи выбрана по дому: широкое событие хранит
    -- повторяющиеся группы Метрики такими же параллельными колонками
    -- (productID, productPrice, productQuantity в ods.event_rep), а не Nested.
    item_sku Array(String),
    item_quantity Array(UInt32),
    item_price Array(Decimal(18, 2)),
    -- Координаты собственной загрузки слоя: каким запуском и когда собран
    -- день. При замене партиции они одинаковы у всех строк дня. Ярлыки ODS
    -- сюда не переезжают — почему, разобрано в доке модели.
    _load_id String,
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY order_date
ORDER BY order_id;

-- Лицо загрузки и диагностики. Ключ шардирования — cityHash64(order_id), тот
-- же, что у ods.order_dist: заказ остаётся на одном шарде на всём пути через
-- слои, и соединения по зерну заказа обходятся без GLOBAL
-- (docs/architecture/storage.md, «Раскладка по шардам»).
CREATE TABLE IF NOT EXISTS dds.order_dist ON CLUSTER clickstream_cluster
AS dds.order_rep
ENGINE = Distributed('clickstream_cluster', 'dds', 'order_rep', cityHash64(order_id));

-- Донор партиций: таблица-двойник, в которой день собирается перед подменой.
--
-- Двойник нужен потому, что REPLACE PARTITION берёт готовый кусок из другой
-- таблицы, а не из результата запроса. Собрать день прямо в цель нельзя:
-- вставка добавила бы строки к уже лежащим, и повтор прогона удвоил бы день.
--
-- Форма двойника задана правилом донора: структура, ключ партиции, ключ
-- сортировки, первичный ключ и политика хранения обязаны совпадать с целью —
-- это требование самой операции (проверено по документации ClickHouse,
-- sql-reference/statements/alter/partition). Отсюда AS dds.order_rep вместо
-- повторённого списка колонок: расхождение становится невозможным по
-- построению и не держится на внимательности того, кто правит.
--
-- Своё имя в keeper двойник получает от макроса {table}, поэтому две таблицы
-- не делят путь репликации.
CREATE TABLE IF NOT EXISTS dds.order_stage_rep ON CLUSTER clickstream_cluster
AS dds.order_rep
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY order_date
ORDER BY order_id;

-- Двойник шардируется тем же ключом, что цель. REPLACE PARTITION идёт по
-- локальным таблицам, шард за шардом. Ляг строка заказа в
-- доноре на другой шард, чем её место в цели, — подмена перенесла бы её не
-- туда, и заказ пропал бы с одного шарда и задвоился на другом.
CREATE TABLE IF NOT EXISTS dds.order_stage_dist ON CLUSTER clickstream_cluster
AS dds.order_stage_rep
ENGINE = Distributed('clickstream_cluster', 'dds', 'order_stage_rep', cityHash64(order_id));

-- Локальная таблица сессии.
--
-- Зерно — визит: подряд идущие события одной куки без пауз длиннее тридцати
-- минут, не пересекающие границу суток (CONTEXT.md, «Визит»). Сущность
-- расчётная, в потоке её нет — слой режет и чеканит её сам
-- (docs/architecture/dds/session.md).
--
-- Партиция — день сессии. У заказа день дышит внутри окна изменяемости, у
-- сессии замерзает сразу: визит суток не пересекает, поэтому прожитый день
-- собирается один раз и целиком. Отсюда и цена отказа: пропущенный день
-- заказа вернуло бы в работу следующее окно, а день сессии вернёт только счёт
-- хвоста от первой дыры (sql/dds/session_scope.sql).
--
-- Ключ сортировки — кука и начало сессии: тем же порядком идёт и нарезка, и
-- чтение «визиты посетителя подряд». session_id в ключ не входит: он ручка
-- для витрин и разбора расхождений, а не адрес строки.
CREATE TABLE IF NOT EXISTS dds.session_rep ON CLUSTER clickstream_cluster
(
    -- Суррогат слоя: cityHash64 от куки и начала сессии. Детерминированность
    -- несущая — пересборка партиции обязана отчеканить те же id
    -- (docs/architecture/dds/session.md, «Идентификатор»).
    session_id UInt64,
    client_id UInt64,
    session_date Date,
    started_at DateTime('Europe/Samara'),
    finished_at DateTime('Europe/Samara'),
    duration_seconds UInt32,
    -- Счётчики по типам событий: у визита Метрики на этом месте достигнутые
    -- цели, которых стенд не шлёт.
    events_total UInt32,
    pageviews UInt32,
    cart_adds UInt32,
    purchases UInt32,
    -- Вход и выход визита, как StartURL и EndURL у Метрики. Полного списка
    -- страниц нет и у неё: топы считает витрина по событиям.
    entry_url String,
    exit_url String,
    -- Атрибуция визита — метки его первого события.
    utm_source String,
    utm_medium String,
    utm_campaign String,
    utm_content String,
    utm_term String,
    -- Паспорт куки: устройство и город приписаны ей на всю жизнь, поэтому
    -- берутся с первого события и внутри визита не меняются.
    device_category LowCardinality(String),
    region_city String,
    _load_id String,
    _load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY session_date
ORDER BY (client_id, started_at);

-- Лицо загрузки и диагностики. Ключ шардирования — cityHash64(client_id), то
-- же выражение, что у событий и у карты идентичностей
-- (docs/architecture/storage.md, «Раскладка по шардам»). Ко-локация здесь не
-- только про соединения: на ней стоит сама сборка — все события куки лежат на
-- одном шарде, и нарезать их можно, не собирая куку с двух нод
-- (sql/dds/session_rebuild.sql).
CREATE TABLE IF NOT EXISTS dds.session_dist ON CLUSTER clickstream_cluster
AS dds.session_rep
ENGINE = Distributed('clickstream_cluster', 'dds', 'session_rep', cityHash64(client_id));

-- Донор партиций сессии. Правило донора — в комментарии к dds.order_stage_rep
-- выше: структура и ключи обязаны совпадать с целью, отсюда AS dds.session_rep
-- и тот же ключ шардирования.
CREATE TABLE IF NOT EXISTS dds.session_stage_rep ON CLUSTER clickstream_cluster
AS dds.session_rep
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY session_date
ORDER BY (client_id, started_at);

CREATE TABLE IF NOT EXISTS dds.session_stage_dist ON CLUSTER clickstream_cluster
AS dds.session_stage_rep
ENGINE = Distributed('clickstream_cluster', 'dds', 'session_stage_rep', cityHash64(client_id));
