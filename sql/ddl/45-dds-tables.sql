-- DDS: физическая модель заказа и донор её партиций.
--
-- Зерно — заказ в текущем состоянии, ключ order_id. Историю версий слой не
-- хранит: у неё нет читателя ни в одной витрине, а ODS её и не обещает
-- (docs/architecture/dds/order.md, «Зерно»).
--
-- Идемпотентность здесь устроена иначе, чем в приёме, и это главный урок
-- сущности. В ODS повтор гасит колонка версии: ReplacingMergeTree(updated_at)
-- оставляет позднюю строку. Здесь колонки версии нет вовсе — повтор гасит
-- замена партиции целиком: день собирается заново и подменяется одним
-- атомарным движением. Поэтому каждая строка таблицы — ровно один заказ, а
-- чтению не нужен FINAL.

-- Локальная таблица шарда.
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

-- Двойник шардируется тем же ключом, что цель, и это не украшение: REPLACE
-- PARTITION идёт по локальным таблицам, шард за шардом. Ляг строка заказа в
-- доноре на другой шард, чем её место в цели, — подмена перенесла бы её не
-- туда, и заказ пропал бы с одного шарда и задвоился на другом.
CREATE TABLE IF NOT EXISTS dds.order_stage_dist ON CLUSTER clickstream_cluster
AS dds.order_stage_rep
ENGINE = Distributed('clickstream_cluster', 'dds', 'order_stage_rep', cityHash64(order_id));
