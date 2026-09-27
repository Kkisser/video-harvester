# Распределённая система обнаружения и загрузки видеоматериалов

Учебный стенд состоит из координатора, трёх или более агентов, SQLite-базы,
веб-панели и локального mock-сервиса. Чужие сервисы не используются.

## Требования

- Python 3.11 или новее;
- OpenSSL для учебного сертификата;
- системный `ssh`, только если используются удалённые серверы.

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp config.example.yaml config.yaml
mkdir -p certs data
```

## Секреты

Создайте разные значения. Не добавляйте их в Git и не записывайте в YAML:

```bash
export SECRET_KEY="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export AGENT_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export BOOTSTRAP_ADMIN_USERNAME="admin"
export BOOTSTRAP_ADMIN_PASSWORD="сложный-пароль-администратора"
export BOOTSTRAP_OPERATOR_PASSWORD="сложный-пароль-оператора"
export BOOTSTRAP_VIEWER_PASSWORD="сложный-пароль-наблюдателя"
```

Во всех терминалах агентов должен быть одинаковый `AGENT_TOKEN`.

## Учебный TLS-сертификат

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
  -keyout certs/key.pem -out certs/cert.pem -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

## Запуск

Терминал 1 — безопасная локальная мишень:

```bash
source .venv/bin/activate
uvicorn mock_target.server:app --host 127.0.0.1 --port 9000
```

Терминал 2 — координатор по HTTPS:

```bash
source .venv/bin/activate
uvicorn coordinator.app:app --host 0.0.0.0 --port 8000 \
  --ssl-keyfile certs/key.pem --ssl-certfile certs/cert.pem
```

Откройте `https://localhost:8000`. Для учебного самоподписанного сертификата
браузер один раз попросит подтвердить исключение.

Создать и запустить задание можно через веб или API:

```bash
curl --cacert certs/cert.pem -u admin:"$BOOTSTRAP_ADMIN_PASSWORD" \
  -H "Content-Type: application/json" -X POST https://localhost:8000/api/jobs \
  -d '{"name":"demo","range_text":"[0,300]","chunk_size":30,"global_rate_limit_rps":30}'

curl --cacert certs/cert.pem -u admin:"$BOOTSTRAP_ADMIN_PASSWORD" \
  -X POST https://localhost:8000/api/jobs/1/start
```

Терминалы 3–5 — три рабочих сервера. Для локального стенда это три процесса:

```bash
STORAGE_PATH=./data/worker-01 python -m agent.agent worker-01 1 --threads 4
STORAGE_PATH=./data/worker-02 python -m agent.agent worker-02 1 --threads 6
STORAGE_PATH=./data/worker-03 python -m agent.agent worker-03 1 --threads 8
```

Каждый агент должен видеть `AGENT_TOKEN`. Пути можно заменить отдельными дисками.

## Проверка отказоустойчивости

1. Дождитесь, когда три агента получат разные блоки.
2. Остановите `worker-02` сочетанием `Ctrl+C`.
3. Подождите больше `lease_seconds`.
4. Просроченный блок вернётся в `pending` и будет выдан другому агенту.
5. Запустите `worker-02` снова. Уже подтверждённые элементы не дублируются.

## Изменение нагрузки без остановки

На странице «Серверы» измените число потоков. Новое значение придёт агенту
в ответе на heartbeat. Общий RPS меняется в карточке задания и делится между
активными агентами при следующей выдаче блока.

## Тесты

```bash
python -m pytest tests/test_unit.py tests/test_integration.py -q
python tests/load_test.py --parallel 100 --requests 1000
```

## Удалённый сервер

Для настоящего учебного сервера заранее разместите проект в `remote_path`,
создайте окружение и задайте на сервере `AGENT_TOKEN`. Координатор использует
системный OpenSSH и разрешает только `probe`, `update` и `start`. Приватный ключ
остаётся на сервере координатора и не передаётся браузеру.

## Ограничения

- SQLite подходит для лабораторного стенда с одним координатором. Для промышленной
  нагрузки следует использовать PostgreSQL.
- Глобальный RPS делится между активными агентами приближённо при аренде блока.
- Локальные процессы изображают отдельные рабочие серверы; протокол взаимодействия
  при этом тот же, что и для удалённых машин.
- Файлы mock-сервиса являются тестовыми байтами, а не настоящими видеоматериалами.
