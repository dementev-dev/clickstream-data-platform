FROM grafana/grafana:13.1.1

# Именованный том закрывает /var/lib/grafana, поэтому плагин остаётся в образе.
# Фоновая установка выключена: нужная версия уже закреплена, а каталог образа
# доступен процессу Grafana только для чтения.
USER root
ENV GF_PATHS_PLUGINS=/usr/share/grafana/plugins \
    GF_PLUGINS_PREINSTALL_DISABLED=true
RUN grafana cli --pluginsDir "$GF_PATHS_PLUGINS" \
    plugins install grafana-clickhouse-datasource 4.20.0
USER grafana
