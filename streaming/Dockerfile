# Shared image for both the producer and consumer services -- they share
# the same codebase and dependencies, only the run command differs
# (set via docker-compose's `command:` per service, not baked in here).
FROM python:3.12-slim

# Print statements in producer.py/consumer.py appear immediately in
# `docker compose logs` instead of sitting in a buffer -- without this,
# stdout is fully buffered when not attached to a TTY (the normal case
# for a detached container), so logs can appear minutes late or not at
# all until the buffer fills.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first so this layer is cached across rebuilds that
# only touch application code, not requirements.
COPY requirements-streaming.txt .
RUN pip install --no-cache-dir -r requirements-streaming.txt

# Everything the streaming module needs at runtime: its own package, the
# shared storage/gold helpers it imports (MinioClient, WarehouseConfig),
# and the data_generator package (TransactionGenerator needs BaseGenerator).
# source_db/ and transaction_data.csv are NOT copied in -- the producer now
# reads live account data from source_db via a runtime volume mount (see
# docker-compose), not a baked-in snapshot.
COPY streaming/ ./streaming/
COPY storage/ ./storage/
COPY data_generator/ ./data_generator/
COPY gold/postgres_loader.py ./gold/postgres_loader.py
COPY gold/__init__.py ./gold/__init__.py

# No default CMD -- docker-compose specifies `python -m streaming.producer`
# or `python -m streaming.consumer` explicitly per service, so a missing
# command override fails loudly instead of silently running the wrong one.