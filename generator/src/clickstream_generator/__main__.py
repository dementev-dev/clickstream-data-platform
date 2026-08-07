"""Точка входа пакета: `python -m clickstream_generator`.

Код возврата уходит наружу как есть — по нему судит о прогоне и контейнер, и
даг, который его запустит (спека генератора, раздел 9).
"""

from clickstream_generator.cli import main

raise SystemExit(main())
