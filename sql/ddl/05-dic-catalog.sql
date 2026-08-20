-- Каталог товаров: подложка на файловом движке и словарь поверх неё.
--
-- Словарь читает не файл, а таблицу хранилища. Это намеренное усложнение ради
-- урока: в бою справочник приезжает процессом, и предложение SOURCE с запросом
-- и учётной записью — та форма, которую менти встретит. Доводы, отвергнутые
-- варианты и условия пересмотра — ADR 0012.

-- Подложка ничего не хранит: движок File перечитывает CSV на каждом запросе,
-- поэтому правка каталога доезжает до словаря сама. Путь считается от
-- user_files, а не от корня данных. Типы повторяют файл, а не модель: цена
-- лежит целыми копейками, приведение делает словарь.
CREATE TABLE IF NOT EXISTS dic.products_file ON CLUSTER clickstream_cluster
(
    sku String,
    name String,
    category String,
    brand String,
    price Int64,
    demand String
)
ENGINE = File(CSVWithNames, './catalog/products.csv');

-- Пользователь dict умеет одно — читать dic. Назвать его обязательно: без user
-- словарь идёт как default с пустым паролем и падает. Хост локальный, поэтому
-- запрос к подложке идёт без сети.
--
-- Форма query, а не table: словарь приводит копейки каталога к Decimal(18, 2)
-- прямо на входе, то есть нормализует, а не зеркалит подложку. Ключ строковый,
-- поэтому COMPLEX_KEY_HASHED: числовой FLAT здесь неприменим.
--
-- Окно вместо LIFETIME(0): словарь обновляется сам, а цена этому — ноды
-- расходятся почти на всё окно (ADR 0012).
CREATE DICTIONARY IF NOT EXISTS dic.products ON CLUSTER clickstream_cluster
(
    sku String,
    name String,
    category String,
    brand String,
    price Decimal(18, 2),
    demand String
)
PRIMARY KEY sku
SOURCE(CLICKHOUSE(
    host 'localhost' port 9000 user 'dict'
    query 'SELECT sku, name, category, brand, toDecimal64(price, 2) / 100 AS price, demand FROM dic.products_file'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 60 MAX 90);
