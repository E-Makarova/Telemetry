# Telemetry demo shortcuts for Windows PowerShell
# In E:\Telemetry, load once with: . .\demo.ps1

# Fix Cyrillic output in the current PowerShell session.
try { chcp 65001 > $null } catch {}
$utf8 = New-Object System.Text.UTF8Encoding $false
[Console]::InputEncoding = $utf8
[Console]::OutputEncoding = $utf8
$OutputEncoding = $utf8
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'

function helpdemo {
    Write-Host "Команды демонстрации телеметрии:" -ForegroundColor Cyan
    Write-Host "  run60    - запустить сбор на 60 секунд"
    Write-Host "  tables   - показать таблицы SQLite"
    Write-Host "  stats    - количество записей в основных таблицах"
    Write-Host "  current  - показать 10 текущих значений со временем обновления"
    Write-Host "  errors   - показать последние 10 ошибок валидации со временем"
    Write-Host "  trend    - вывести последние 20 записей trend_001"
    Write-Host "  trend20  - вывести последние 20 записей trend_020"
    Write-Host "  wal      - проверить WAL трендов"
    Write-Host "  recover  - запустить восстановление трендов"
    Write-Host "  tests    - запустить тесты pytest"
}

function run60 { python -m telemetry.main --config config.json --run --seconds 60 }
function wal { python -m telemetry.tools.inspect_wal --config config.json }
function recover { python -m telemetry.tools.recover_trends --config config.json }
function trend { python -m telemetry.tools.dump_trend --config config.json --trend-id trend_001 }
function trend20 { python -m telemetry.tools.dump_trend --config config.json --trend-id trend_020 }
function tests { pytest -v }

function tables {
@'
import os, sqlite3
p = os.path.join(os.environ["TEMP"], "TelemetrySQLiteSystem", "data", "telemetry.sqlite3")
con = sqlite3.connect("file:" + p.replace("\\", "/") + "?mode=ro", uri=True)
print("ТАБЛИЦЫ БАЗЫ ДАННЫХ:")
for (name,) in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    print(" -", name)
con.close()
'@ | python -
}

function stats {
@'
import os, sqlite3
p = os.path.join(os.environ["TEMP"], "TelemetrySQLiteSystem", "data", "telemetry.sqlite3")
con = sqlite3.connect("file:" + p.replace("\\", "/") + "?mode=ro", uri=True)
print("СТАТИСТИКА БАЗЫ ДАННЫХ:")
for table in ["sources", "telemetry_objects", "current_values", "telemetry_values", "invalid_values", "trends", "trend_state"]:
    n = con.execute('SELECT COUNT(*) FROM "' + table + '"').fetchone()[0]
    print(f"{table}: {n:,}".replace(",", " "))
con.close()
'@ | python -
}

function current {
@'
import os, sqlite3
p = os.path.join(os.environ["TEMP"], "TelemetrySQLiteSystem", "data", "telemetry.sqlite3")
con = sqlite3.connect("file:" + p.replace("\\", "/") + "?mode=ro", uri=True)
con.row_factory = sqlite3.Row
sql = """SELECT cv.source_id, cv.object_id, o.name, cv.scalar_type,
CASE cv.scalar_type WHEN 'integer' THEN CAST(cv.int_value AS TEXT)
WHEN 'float' THEN CAST(cv.float_value AS TEXT)
WHEN 'boolean' THEN CASE cv.bool_value WHEN 1 THEN 'true' ELSE 'false' END END AS value,
cv.updated_ts
FROM current_values cv LEFT JOIN telemetry_objects o
ON o.source_id=cv.source_id AND o.object_id=cv.object_id
ORDER BY cv.source_id, cv.object_id LIMIT 10"""
from datetime import datetime, timezone
print("ПЕРВЫЕ 10 ТЕКУЩИХ ЗНАЧЕНИЙ (время обновления):")
for r in con.execute(sql):
    ts = datetime.fromtimestamp(r['updated_ts'] / 1_000_000_000, tz=timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(f"{ts} | источник={r['source_id']} | объект={r['object_id']} | {r['name']} | {r['scalar_type']} | {r['value']}")
con.close()
'@ | python -
}

function errors {
@'
import os, sqlite3
p = os.path.join(os.environ["TEMP"], "TelemetrySQLiteSystem", "data", "telemetry.sqlite3")
con = sqlite3.connect("file:" + p.replace("\\", "/") + "?mode=ro", uri=True)
from datetime import datetime, timezone
print("ПОСЛЕДНИЕ 10 ОШИБОК ВАЛИДАЦИИ (время получения):")
for r in con.execute("SELECT id, source_id, object_id, received_ts, raw_value, reason FROM invalid_values ORDER BY id DESC LIMIT 10"):
    ts = datetime.fromtimestamp(r[3] / 1_000_000_000, tz=timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(f"{ts} | id={r[0]} | источник={r[1]} | объект={r[2]} | значение={r[4]} | причина={r[5]}")
con.close()
'@ | python -
}

Write-Host "Команды загружены. Введи helpdemo для списка." -ForegroundColor Green
