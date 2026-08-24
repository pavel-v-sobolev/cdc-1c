# Образ ставит cdc-1c с PyPI по версии: версия пакета = версия образа, собирать нечего.
#   docker build --build-arg CDC1C_VERSION=0.1.18 -t sobolevp/cdc-1c:0.1.18 .
# Версия должна быть уже опубликована на PyPI, иначе pip внутри сборки её не найдёт.
FROM python:3.13-slim

ARG CDC1C_VERSION
LABEL org.opencontainers.image.title="cdc-1c" \
      org.opencontainers.image.description="Change data capture (CDC) from 1C:Enterprise to your data warehouse" \
      org.opencontainers.image.source="https://github.com/pavel-v-sobolev/cdc_1C" \
      org.opencontainers.image.version="${CDC1C_VERSION}" \
      org.opencontainers.image.licenses="MIT"

# tzdata: расписания FullLoadCron считаются в локальном времени (TZ), а в slim-образе базы зон нет.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

# PYTHONUNBUFFERED: логи не залипают в буфере при `docker logs`.
# PYTHONDONTWRITEBYTECODE: не пытаемся писать __pycache__ в смонтированный (обычно ro) /config.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Экстра postgres — psycopg2-binary, колесо, поэтому компилятор и dev-заголовки в образе не нужны.
RUN test -n "${CDC1C_VERSION}" || (echo "build-arg CDC1C_VERSION is required" >&2; exit 1) \
    && pip install --no-cache-dir "cdc-1c[postgres]==${CDC1C_VERSION}"

# Шаблон конфига внутри образа: достаётся из него же, версия шаблона совпадает с версией библиотеки.
#   docker run --rm sobolevp/cdc-1c:<version> tar c -C /opt/cdc-1c config | tar x
COPY config /opt/cdc-1c/config
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh

RUN useradd --create-home --uid 1000 cdc
USER cdc

# Сюда монтируется пользовательский конфиг; не смонтирован — работает env-режим (см. entrypoint).
WORKDIR /config

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
