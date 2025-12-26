# Alfresco Community – AI Event Processing Pipeline

This project implements a **robust, event-driven AI processing pipeline** for **Alfresco Community Edition** using **ActiveMQ, Python, Celery, and Redis**.

The system reacts to **repository events** (such as file uploads), processes them asynchronously using **AI workers**, and guarantees **reliability, scalability, and no data loss** — without using any Enterprise-only Alfresco features.

---

## ✨ Key Features

- Reacts to Alfresco **node create / content ready events**
- External **AI processing pipeline** (auto-tagging, embeddings, metadata, etc.)
- **No blocking** inside Alfresco repository
- **At-least-once delivery** with broker-managed retries
- **Idempotent workers** (safe re-delivery)
- Fully **on-prem and AWS deployable**
- Clean separation of concerns
- Production-grade Docker setup

---

## 🧠 High-Level Architecture

Alfresco Repository (Community)
|
| emits EVENT (fact)
v
ActiveMQ Queue / Topic
|
| CLIENT_ACK
v
Python STOMP Consumer
|
| dispatches task
v
Celery Workers
|
| AI processing
v
(Tags / Metadata / Vector DB)


**Important design rule**:

> Alfresco announces *what happened*.  
> Python decides *what to do*.  
> Workers do the *heavy work*.

---

## 📦 Project Structure

alfresco-ai-pipeline/
│
├── docker/
│ ├── docker-compose.yml
│ ├── consumer.Dockerfile
│ ├── worker.Dockerfile
│ └── .dockerignore
│
├── consumer/
│ ├── main.py # STOMP consumer entrypoint
│ ├── stomplistener.py # Message handling + ACK logic
│ └── schema.py # Event schema (Pydantic)
│
├── workers/
│ ├── celery_app.py # Celery configuration
│ ├── tasks.py # AI processing tasks
│ └── idempotency.py # Redis-based idempotency
│
├── settings.py # Shared environment-based config
├── requirements.txt
├── .env # Local-only (not committed)
├── .gitignore
└── README.md


---

## 🔐 Configuration Model (IMPORTANT)

This project follows **12-factor app principles**:

- **Application code reads ONLY environment variables**
- **Docker Compose injects variables**
- `.env` is **never read by Python directly**
- `.env` is **not included in Docker images**

---

## 🧾 Environment Variables

Create a `.env` file in the **project root**:

```env
# ActiveMQ
ACTIVEMQ_HOST=activemq
ACTIVEMQ_PORT=61613
ACTIVEMQ_USER=admin
ACTIVEMQ_PASSWORD=admin
ACTIVEMQ_QUEUE=/queue/alfresco.upload.events
ACTIVEMQ_PREFETCH=1
ACTIVEMQ_HEARTBEAT_OUT=10000
ACTIVEMQ_HEARTBEAT_IN=10000

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Celery
CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/1

# Worker
WORKER_TIMEOUT=600
```

## 🐳 Running with Docker Compose
Prerequisites

Docker ≥ 20.x

Docker Compose v2

Alfresco Community already running (or ActiveMQ reachable)

Start the stack

From the project root directory:

docker compose --env-file .env -f docker/docker-compose.yaml up --build


This will start:

ActiveMQ (STOMP enabled)
Redis
Python STOMP consumer
Celery worker(s)

Scale workers
docker compose --env-file .env -f docker/docker-compose.yaml up --scale worker=2
