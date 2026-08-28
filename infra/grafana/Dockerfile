FROM grafana/grafana:13.1.1

# Именованный том закрывает /var/lib/grafana, поэтому плагин остаётся в образе.
# Фоновая установка выключена: нужная версия уже закреплена, а каталог образа
# доступен процессу Grafana только для чтения.
USER root
ENV GF_PATHS_PLUGINS=/usr/share/grafana/plugins \
    GF_PLUGINS_PREINSTALL_DISABLED=true
ARG CLICKHOUSE_PLUGIN_VERSION=4.20.0
RUN grafana cli --pluginsDir "$GF_PATHS_PLUGINS" \
    --pluginUrl "https://github.com/grafana/clickhouse-datasource/releases/download/v${CLICKHOUSE_PLUGIN_VERSION}/grafana-clickhouse-datasource-${CLICKHOUSE_PLUGIN_VERSION}.zip" \
    plugins install grafana-clickhouse-datasource "$CLICKHOUSE_PLUGIN_VERSION"
USER grafana
