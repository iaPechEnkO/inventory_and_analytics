# Inventory & Analytics — склад с Redis Pub/Sub

Два асинхронных сервиса для складских операций:

- inventory_api — HTTP API (aiohttp): принимает операции поступления/расхода, публикует события в Redis, отдаёт остатки и аналитику из PostgreSQL.
- analytics_worker — подписчик Redis Pub/Sub: слушает inventory.receipt и inventory.issue, валидирует события и атомарно обновляет агрегаты в stock_agg.

Хранилище — PostgreSQL, доступ через SQLAlchemy Core (не ORM) поверх asyncpg. Коммуникация между сервисами — Redis Pub/Sub.

---

## Стек

- Python 3.11
- aiohttp 3.14
- SQLAlchemy 2.1 Core + asyncpg
- redis-py (asyncio)
- PostgreSQL 16, Redis 7
- pytest + pytest-asyncio + testcontainers
- Docker / docker compose

---

## Структура репозитория

    .
    ├── analytics_worker/        # Сервис 2: подписчик Redis Pub/Sub, обновляет stock_agg
    │   ├── main.py
    │   └── worker_logic.py
    ├── inventory_api/           # Сервис 1: HTTP API, публикует события
    │   ├── api.py
    │   ├── inventory_logic.py
    │   └── main.py
    ├── connections/
    │   └── redis_connector.py
    ├── database/
    │   ├── migrations/
    │   │   └── init.sql         # Схема для docker-compose (Postgres инициализируется при первом старте)
    │   ├── db_connector.py
    │   └── tables.py            # SQLAlchemy Core-описание таблиц
    ├── settings/
    │   ├── config.py
    │   └── logger.py
    ├── tests/
    │   ├── conftest.py
    │   ├── test_api.py
    │   └── test_integration.py
    ├── .github/workflows/ci.yml
    ├── README.md
    ├── .env.example
    ├── .gitignore
    ├── .dockerignore
    ├── docker-compose.yml
    ├── Dockerfile
    ├── pytest.ini
    ├── requirements.txt
    └── requirements-dev.txt

---

## Быстрый старт (Docker Compose)

Требуется Docker Desktop (Windows/macOS) или docker + docker compose (Linux).

    docker compose up -d --build

Поднимаются 4 контейнера:

| Сервис    | Порт на хосте | Описание                                  |
|-----------|---------------|-------------------------------------------|
| postgres  | 5432          | PostgreSQL 16, БД inventory               |
| redis     | 6379          | Redis 7                                   |
| api       | 8080          | HTTP API                                  |
| worker    | —             | Подписчик Redis Pub/Sub                   |

Схема БД применяется автоматически из database/migrations/init.sql (монтируется в docker-entrypoint-initdb.d).

Проверка:

    curl http://localhost:8080/stock
    # {"items": []}

Внимание: конфликт портов. Если на хосте уже запущены локальные Postgres/Redis на 5432/6379, docker compose up упадёт с "port is already allocated". Остановите локальные службы ИЛИ поменяйте проброс в docker-compose.yml (например, "5433:5432" и "6380:6379").

---

## Локальный запуск (без Docker)

Требуются запущенные PostgreSQL и Redis, доступные на localhost.

1. Создайте БД и роль (один раз):

    - CREATE USER "user" WITH PASSWORD 'pass';
    - CREATE DATABASE inventory OWNER "user";

2. Создайте виртуальное окружение и установите зависимости:

    - python -m venv .venv 
    - source .venv/bin/activate        # Linux/macOS 
    - .\.venv\Scripts\Activate.ps1   # Windows PowerShell 
    - pip install -r requirements-dev.txt 

3. Скопируйте .env.example в .env и при необходимости поправьте креды:

    - cp .env.example .env

4. Примените схему:

    - psql -h localhost -U user -d inventory -f database/migrations/init.sql

5. Запустите сервисы (в двух терминалах):

    - python -m inventory_api.main
    - python -m analytics_worker.main

API слушает http://0.0.0.0:8080.

---

## Переменные окружения

Все переменные читаются из .env (см. .env.example) или из окружения.

| Переменная                  | По умолчанию                  | Описание                                       |
|-----------------------------|-------------------------------|------------------------------------------------|
| API_HOST                    | 0.0.0.0                       | Адрес API                                      |
| API_PORT                    | 8080                          | Порт API                                       |
| PG_HOST / PG_PORT           | localhost / 5432              | Подключение к PostgreSQL                       |
| PG_USER / PG_PASSWORD       | user / pass                   | Креды PostgreSQL                               |
| PG_DB                       | inventory                     | Имя БД                                         |
| PG_POOL_SIZE                | 3                             | Размер пула SQLAlchemy                         |
| REDIS_HOST / REDIS_PORT     | localhost / 6379              | Подключение к Redis                            |
| REDIS_DB                    | 0                             | Номер БД Redis                                 |
| REDIS_POOL_SIZE             | 3                             | Размер пула Redis                              |
| USE_CONTAINERS_FOR_TESTS    | false                         | true → тесты поднимают свои контейнеры (testcontainers); false → используют PG_*/REDIS_* |

---

## API

### POST /receipts — поступление

    curl -X POST http://localhost:8080/receipts \
      -H "Content-Type: application/json" \
      -d '{"sku":"A-100","qty":50,"warehouse":"WH-1"}'

Ответ: 201 {"status":"ok"}

Действия: пишет строку в stock_events с event_type='receipt', публикует событие в канал inventory.receipt.

### POST /issues — расход

    curl -X POST http://localhost:8080/issues \
      -H "Content-Type: application/json" \
      -d '{"sku":"A-100","qty":20,"warehouse":"WH-1"}'

Ответ: 201 {"status":"ok"}

Действия: пишет строку в stock_events с event_type='issue', публикует событие в канал inventory.issue.

Валидация (оба эндпоинта): qty — положительное целое, sku и warehouse — непустые строки. При нарушении — 422.

### GET /stock — текущие остатки

Параметры (опционально): warehouse, sku.

    curl "http://localhost:8080/stock?warehouse=WH-1"

    {"items": [{"warehouse": "WH-1", "sku": "A-100", "qty": 30}]}

Остаток считается агрегацией stock_events (receipt — +qty, issue — -qty) через GROUP BY warehouse, sku во вложенном подзапросе.

### GET /stock/summary — агрегированная статистика

Параметр top_n (по умолчанию 5) управляет размером списка top_skus.

    curl "http://localhost:8080/stock/summary?top_n=3"

    {
      "warehouses": [
        {"warehouse": "WH-1", "total_qty": 90, "sku_count": 2},
        {"warehouse": "WH-2", "total_qty": 40, "sku_count": 2}
      ],
      "top_skus": [
        {"sku": "B-200", "total_qty": 60},
        {"sku": "A-100", "total_qty": 40},
        {"sku": "C-300", "total_qty": 30}
      ]
    }

- warehouses[i].total_qty — суммарный остаток по складу;
- warehouses[i].sku_count — количество уникальных SKU на складе;
- top_skus — глобальный TOP-N SKU по суммарному остатку по всем складам.

Некорректный top_n (0, отрицательное, не число) → 422.

---

## Worker

- Подписывается на inventory.receipt и inventory.issue.
- На старте выполняет seed_agg — пересчитывает stock_agg из stock_events (идемпотентно, через INSERT ... ON CONFLICT DO UPDATE).
- Для каждого события валидирует JSON и поля (sku: str, qty: positive int, warehouse: str). Невалидные сообщения логируются и пропускаются — worker продолжает работу.
- Обновляет stock_agg атомарным upsert'ом (INSERT ... ON CONFLICT (warehouse, sku) DO UPDATE SET qty = stock_agg.qty + delta), поэтому событие не теряется, даже если строки ещё нет. INSERT-часть срабатывает только если строки ещё нет (например, для пары, впервые появившейся после seed_agg).

---

## Схема БД

stock_events — все операции:

| Колонка      | Тип          |
|--------------|--------------|
| id           | BIGSERIAL PK |
| sku          | VARCHAR(64)  |
| qty          | INTEGER      |
| warehouse    | VARCHAR(64)  |
| event_type   | VARCHAR(16)  — receipt / issue |
| created_at   | TIMESTAMPTZ DEFAULT NOW() |

Индекс: (warehouse, sku).

stock_agg — агрегаты:

| Колонка     | Тип         |
|-------------|-------------|
| warehouse   | VARCHAR(64) PK |
| sku         | VARCHAR(64) PK |
| qty         | INTEGER NOT NULL DEFAULT 0 |

---

## Тесты

    pytest tests/ -v

Покрытие:

- tests/test_api.py — эндпоинты API: создание событий, публикация в Redis, валидация, GET /stock, GET /stock/summary, top_n.
- tests/test_integration.py — worker: обработка событий из Redis, seed_agg, идемпотентность, upsert для новой пары.

Два режима:

| USE_CONTAINERS_FOR_TESTS | Что происходит |
|--------------------------|----------------|
| false (по умолчанию)     | Тесты идут в PG_*/REDIS_* (локальные сервисы). Docker не нужен. |
| true                     | testcontainers поднимает изолированные контейнеры Postgres 16 и Redis 7 на случайных портах. Нужен работающий Docker. |

Оба режима проходят локально (11/11). В CI используется false — Postgres и Redis поднимаются как сервисы GitHub Actions.
