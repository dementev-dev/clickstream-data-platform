-- Общий с генератором каталог товаров. ClickHouse разрешает файловому
-- источнику читать только из user_files; Compose монтирует сюда один и тот же
-- файл на обе ноды. Цена хранится в копейках, как и в контракте события.
CREATE DICTIONARY IF NOT EXISTS dds.products ON CLUSTER clickstream_cluster
(
    sku String,
    name String,
    category String,
    brand String,
    price Int64,
    demand String
)
PRIMARY KEY sku
SOURCE(FILE(PATH './user_files/catalog/products.csv' FORMAT 'CSVWithNames'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(0);
