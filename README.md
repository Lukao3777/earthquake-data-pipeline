# Real-Time Earthquake Data Pipeline

A real-time Data Engineering project that collects live earthquake data from the USGS API, streams it through Apache Kafka, processes it with Apache Spark Structured Streaming, and stores the results in PostgreSQL.

The goal is to build something beyond a static CSV pipeline and work with a real streaming architecture.

> 🚧 Project in progress  
> Core pipeline is working. Power BI, tests, and final documentation are still being added.

---

## Architecture

```text
USGS API
   ↓
Python Producer
   ↓
Apache Kafka
   ↓
Apache Spark
   ↓
PostgreSQL
   ↓
Power BI
```

---

## Tech Stack

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- PostgreSQL
- Docker
- Docker Compose
- USGS Earthquake API
- Power BI
- Git / GitHub

---

## How It Works

1. Python retrieves earthquake data from the USGS API.
2. Each earthquake is transformed into a standardized JSON structure.
3. The producer sends the events to Kafka.
4. Spark Structured Streaming consumes the Kafka topic.
5. Spark parses and transforms the data.
6. Duplicate earthquakes are filtered using `event_id`.
7. New events are stored in PostgreSQL.
8. Spark checkpoints preserve streaming progress between restarts.
9. PostgreSQL will be used as the source for a Power BI dashboard.

---

## Project Structure

```text
earthquake-data-pipeline/
│
├── ingestion/
│   └── producer.py
│
├── spark/
│   └── processor.py
│
├── checkpoints/      # ignored by Git
├── .env              # ignored by Git
├── .env.example
├── .gitignore
├── compose.yml
├── requirements.txt
└── README.md
```

---

## Data Source

This project uses the official USGS earthquake feed:

```text
https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson
```

The endpoint returns earthquakes detected during the last hour.

Because this is a rolling snapshot, the same earthquake may appear in multiple API requests. The pipeline uses the USGS `event_id` to avoid duplicate records.

---

## Event Example

```json
{
  "event_id": "ci41333215",
  "magnitude": 1.26,
  "place": "11 km SE of Running Springs, CA",
  "longitude": -117.0275,
  "latitude": 34.1406,
  "depth_km": 9.06,
  "event_time": 1789497246600
}
```

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/Lukao3777/earthquake-data-pipeline.git
cd earthquake-data-pipeline
```

### 2. Create the virtual environment

#### Windows

```powershell
python -m venv .venv
.venv\Scripts\activate
```

#### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file based on `.env.example`:

```env
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=your_database_name
```

Example:

```env
POSTGRES_USER=earthquake_user
POSTGRES_PASSWORD=earthquake_password
POSTGRES_DB=earthquakes
```

### 5. Start the infrastructure

```bash
docker compose up -d
```

Check the services:

```bash
docker compose ps
```

### 6. Create the Kafka topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic earthquakes \
  --partitions 1 \
  --replication-factor 1
```

### 7. Create the PostgreSQL table

```bash
docker compose exec postgres psql -U earthquake_user -d earthquakes
```

```sql
CREATE TABLE earthquakes (
    event_id VARCHAR(50) PRIMARY KEY,
    magnitude DOUBLE PRECISION,
    place TEXT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    depth_km DOUBLE PRECISION,
    event_time BIGINT
);
```

### 8. Run the producer

```bash
python ingestion/producer.py
```

Example output:

```text
Earthquakes found: 7
{'event_id': 'nc75436147', 'magnitude': 0.75, ...}
```

### 9. Check Spark processing

```bash
docker compose logs spark --tail 30
```

Example:

```text
Batch 2: 7 novos terremotos gravados.
```

### 10. Check the stored data

```bash
docker compose exec postgres psql -U earthquake_user -d earthquakes
```

```sql
SELECT
    event_id,
    magnitude,
    place,
    depth_km
FROM earthquakes
ORDER BY event_time DESC
LIMIT 10;
```

---

## Duplicate Handling

The pipeline prevents duplicate earthquakes in two ways:

- Spark removes duplicated `event_id` values inside each batch.
- Existing PostgreSQL IDs are compared using a `LEFT ANTI JOIN`.

The PostgreSQL table also uses:

```sql
event_id PRIMARY KEY
```

which provides an additional uniqueness constraint.

---

## Streaming Checkpoints

Spark stores its streaming progress in:

```text
/opt/spark/checkpoints/earthquakes
```

This path is mapped to:

```text
./checkpoints
```

The checkpoint allows Spark to continue from the last processed Kafka offset after a restart.

---

## Current Status

### Completed

- [x] USGS API ingestion
- [x] Python producer
- [x] Apache Kafka
- [x] Spark Structured Streaming
- [x] JSON parsing and transformation
- [x] PostgreSQL storage
- [x] Duplicate handling
- [x] Persistent checkpoints
- [x] Dockerized infrastructure
- [x] Environment variables
- [x] GitHub repository

### Next Steps

- [ ] Power BI dashboard
- [ ] Automated tests
- [ ] Architecture diagram
- [ ] Final screenshots
- [ ] Final documentation cleanup

---

## Planned Power BI Dashboard

The dashboard will include metrics such as:

- Total earthquakes
- Average magnitude
- Maximum magnitude
- Average depth
- Earthquakes by location
- Earthquake activity over time
- Most recent events
- Geographic map

---

## Future Improvements

Possible improvements:

- PostgreSQL UPSERT using `ON CONFLICT`
- Kafka persistence
- Health checks
- Automated topic creation
- Unit and integration tests
- Data quality validation
- CI/CD
- Cloud deployment

---

## Author

**Lucas**

GitHub: [@Lukao3777](https://github.com/Lukao3777)

---

# Português 🇧🇷

## Sobre o projeto

Este projeto é um pipeline de Engenharia de Dados utilizando dados reais de terremotos da USGS.

O fluxo é:

```text
USGS API
   ↓
Python
   ↓
Kafka
   ↓
Spark
   ↓
PostgreSQL
   ↓
Power BI
```

O Python busca os dados da USGS e envia os eventos para o Kafka.

O Spark Structured Streaming consome as mensagens, transforma os dados, remove eventos duplicados e grava novos registros no PostgreSQL.

O projeto também utiliza checkpoints persistentes para manter o progresso do streaming após reinicializações.

---

## Tecnologias

- Python
- Apache Kafka
- Apache Spark
- PostgreSQL
- Docker
- Docker Compose
- USGS API
- Power BI
- Git / GitHub

---

## Status

Já está funcionando:

- Ingestão de dados reais
- Python → Kafka
- Kafka → Spark
- Spark → PostgreSQL
- Tratamento de duplicados
- Checkpoint persistente
- Variáveis de ambiente
- Docker Compose

Próximas etapas:

- Power BI
- Testes automatizados
- Diagrama final
- Screenshots
- Melhorias finais de documentação

---

## Objetivo

O objetivo principal é praticar conceitos reais de Engenharia de Dados, como streaming, Kafka, Spark, persistência, idempotência, checkpoints e integração entre serviços.