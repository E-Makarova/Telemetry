# Telemetry SQLite System

Система собирает телеметрию из 4 независимых источников-эмуляторов, валидирует скалярные значения, пишет валидные значения в SQLite и ведёт 20 секундных трендов с дисковым sliding window на 3600 значений.

## Архитектура

```text
sources -> bounded input queue -> validator -> bounded valid queue -> SQLite writer
                                             \
                                              -> invalid value log/table

SQLite current_values -> trend scheduler -> custom trend WAL -> ring files
```

SQLite использует собственный механизм `PRAGMA journal_mode=WAL`. Это не тот же механизм, что custom WAL трендов. Custom WAL нужен для восстановления trend ring-файлов после аварийного завершения.

## Структура

```text
telemetry/
  app.py
  config.py
  models.py
  scalar_types.py
  sources/
  validation/
  storage/
  trends/
  wal/
  tools/
tests/
config.json
```

## Типы данных

Поддерживаются скалярные типы:

- `integer`
- `float`
- `boolean`

Типы расширяются через `ScalarTypeRegistry`, без переписывания валидатора и основного pipeline.

## SQLite

Основные таблицы:

- `sources`
- `telemetry_objects`
- `telemetry_values`
- `current_values`
- `trends`
- `trend_state`
- `invalid_values`

Запись телеметрии идёт batch-ами. Для реальной многолетней эксплуатации сырую таблицу `telemetry_values` нужно держать под retention/partitioning. При максимуме 2400 значений/сек обычная история может занимать десятки ГБ в сутки.

## Custom WAL трендов

Файл WAL хранит length-prefixed JSON-записи с CRC32:

```text
uint32 payload_length
json payload
uint32 crc32(payload)
```

Если запись подтверждена `fsync`, но процесс упал до записи в ring-файл, recovery повторно применит запись. Дубли отсеиваются по `sequence` и `trend_state.last_sequence`.

## Тренды

Для каждого тренда используется отдельный ring-файл фиксированного размера. Окно:

```text
20 trends * 3600 values = 72000 samples
```

Размер trend storage не зависит от срока работы программы.

## Runtime-файлы

По умолчанию БД, WAL, ring-файлы и логи лежат в:

```text
%TEMP%/TelemetrySQLiteSystem/
```

Это сделано намеренно: SQLite чувствителен к файловым блокировкам синхронизируемых каталогов вроде OneDrive. Для production лучше указать постоянный локальный каталог в `config.json`, который стабильно поддерживает SQLite locking.

## Установка

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -r requirements-dev.txt
```

## Запуск

```powershell
python -m telemetry.main --config config.json --init-db
python -m telemetry.main --config config.json --run --seconds 60
```

## Проверка

```powershell
pytest
python -m telemetry.tools.inspect_wal --config config.json
python -m telemetry.tools.recover_trends --config config.json
python -m telemetry.tools.dump_trend --config config.json --trend-id trend_001
```

## Нагрузка

Расчёт:

- минимум: `4 * 100 = 400` изменений/сек;
- максимум: `4 * 600 = 2400` изменений/сек;
- максимум в минуту: `144000`;
- максимум в час: `8640000`;
- тренды: `20` записей/сек, `1728000` записей/сутки.

Стабильность достигается за счёт bounded queues, batch insert, одного SQLite writer-а, фиксированного размера trend ring-файлов и регулярной компактации custom WAL.

## Эксплуатация

- Следить за размером `telemetry_values`.
- Настроить retention или архивирование старых партиций.
- Держать базу и ring-файлы на надёжном диске.
- Не отключать custom WAL fsync для production.
- Периодически проверять SQLite checkpoint и логи.
