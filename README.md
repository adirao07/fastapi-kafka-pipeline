# Event Pipeline

FastAPI accepts events and publishes them to Kafka. A consumer persists each event in PostgreSQL, caches it in Redis, and queues a Celery task for side effects.

```mermaid
flowchart LR
  Client -->|"POST /api/v1/events"| API
  API -->|"202 Accepted"| Client
  API -->|publish| Kafka
  Kafka -->|poll events_stream| Consumer
  Consumer --> Postgres
  Consumer -->|event:id TTL 300s| Redis
  Consumer -->|process_event_side_effects| Worker
  Worker --> Postgres
  Worker --> Redis
  Client -->|"GET /api/v1/events/{id}"| API
  API --> Redis
  API --> Postgres
```

## Services

| Service | Role |
| --- | --- |
| `api` | `POST /api/v1/events` publishes to Kafka and returns `202` with `event_id`. `GET /api/v1/events/{event_id}` reads Redis first, then PostgreSQL. |
| `kafka` | Topic `events_stream`. In-cluster clients use `kafka:29092`. The host uses `localhost:9092`. |
| `zookeeper` | Coordination for the Kafka broker. |
| `consumer` | Polls `events_stream`, inserts an `EventRecord`, caches `event:{event_id}` for 300 seconds, and calls `process_event_side_effects.delay(...)`. |
| `celery_worker` | Runs `process_event_side_effects`, marks the row `processed`, and refreshes the Redis entry. |
| `postgres` | Database `pipeline_db` (`pipeline_user` / `pipeline_password`). |
| `redis` | Celery broker and event cache. |

`GET` can return 404 for a short time after `POST`. The id is accepted as soon as Kafka has the message; the row exists only after the consumer commits it.

## Run

Docker Compose builds the app image and starts every service:

```bash
docker compose up --build -d
```

Follow the pipeline logs:

```bash
docker compose logs -f api consumer celery_worker
```

Publish an event:

```powershell
curl.exe -s -X POST http://localhost:8000/api/v1/events `
  -H "Content-Type: application/json" `
  -d '{"event_type":"user.signup","user_id":"user-42","payload":{"plan":"pro"}}'
```

Read it back (wait a moment if the consumer has not committed yet):

```powershell
curl.exe -s http://localhost:8000/api/v1/events/<event_id>
```

Health check: `GET http://localhost:8000/health`.

Stop the stack:

```bash
docker compose down
```

`docker compose down -v` also removes the Postgres volume.

## Configuration

Settings load from the environment via `app.config.Settings`:

| Variable | Default |
| --- | --- |
| `DATABASE_URL` | `postgresql://pipeline_user:pipeline_password@postgres:5432/pipeline_db` |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| `KAFKA_TOPIC` | `events_stream` |
| `REDIS_URL` | `redis://redis:6379/0` |

Compose sets these for `api`, `consumer`, and `celery_worker`. The API and consumer retry Kafka connections while the broker is still starting.
