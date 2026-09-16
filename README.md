# Real-Time Earthquake Data Pipeline

A real-time Data Engineering project that collects live earthquake data from the USGS API, streams it through Apache Kafka, processes it with Apache Spark Structured Streaming, and stores the results in PostgreSQL.

The project was built to go beyond static CSV-based pipelines and explore how data moves through a real streaming architecture.

---

## Architecture

![Pipeline Architecture](architecture/pipeline-architecture.png)

### Data Flow

```text
USGS API
   ↓
Python Producer
   ↓
Apache Kafka
   ↓
Apache Spark Structured Streaming
   ↓
PostgreSQL
   ↓
Power BI
```

The pipeline also uses:

- Persistent Spark checkpoints
- Docker Compose for infrastructure
- Environment variables for configuration and credentials

---

## Dashboard

The processed earthquake data is visualized in Power BI.

![Power BI Dashboard](screenshots/dashboard.png)

The dashboard includes:

- Total earthquakes
- Average magnitude
- Maximum magnitude
- Average depth
- Earthquake activity over time
- Magnitude distribution
- Geographic earthquake locations
- Recent earthquake events

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
- Pytest
- Git / GitHub

---

## How It Works

1. Python retrieves earthquake data from the USGS API.
2. Each earthquake is transformed into a standardized JSON structure.
3. The producer publishes the events to Apache Kafka.
4. Spark Structured Streaming consumes the Kafka topic.
5. Spark parses and transforms the incoming JSON data.
6. Duplicate earthquakes are filtered using the USGS `event_id`.
7. New records are stored in PostgreSQL.
8. Spark checkpoints preserve streaming progress between restarts.
9. Power BI connects to PostgreSQL and visualizes the processed data.

---

## Project Structure

```text
earthquake-data-pipeline/
│
├── architecture/
│   └── pipeline-architecture.png
│
├── ingestion/
│   ├── __init__.py
│   └── producer.py
│
├── powerbi/
│   └── EarthquakeDashboard.pbix
│
├── screenshots/
│   └── dashboard.png
│
├── spark/
│   └── processor.py
│
├── tests/
│   └── test_producer.py
│
├── checkpoints/          # ignored by Git
├── .env                  # ignored by Git
├── .env.example
├── .gitignore
├── compose.yml
├── requirements.txt
└── README.md
```

---

## Data Source

This project uses the official USGS Earthquake Hazards Program feed:

```text
https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson
```

The endpoint returns earthquakes detected during the last hour.

Because the API represents a rolling snapshot, the same earthquake may appear in multiple requests.

The pipeline uses the USGS `event_id` to prevent duplicate records from being inserted into PostgreSQL.

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

# Getting Started

## 1. Clone the repository

```bash
git clone https://github.com/Lukao3777/earthquake-data-pipeline.git
cd earthquake-data-pipeline
```

## 2. Create the virtual environment

### Windows

```powershell
python -m venv .venv
.venv\Scripts\activate
```

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

## 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## 4. Configure environment variables

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

The `.env` file is ignored by Git.

---

## 5. Start the infrastructure

```bash
docker compose up -d
```

Check the services:

```bash
docker compose ps
```

The infrastructure includes:

- Apache Kafka
- Apache Spark
- PostgreSQL

---

## 6. Create the Kafka topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic earthquakes \
  --partitions 1 \
  --replication-factor 1
```

---

## 7. Create the PostgreSQL table

Connect to PostgreSQL:

```bash
docker compose exec postgres psql -U earthquake_user -d earthquakes
```

Create the table:

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

Exit PostgreSQL:

```sql
\q
```

---

## 8. Run the producer

```bash
python ingestion/producer.py
```

Example output:

```text
Earthquakes found: 7
{'event_id': 'nc75436147', 'magnitude': 0.75, ...}
```

---

## 9. Check Spark processing

```bash
docker compose logs spark --tail 30
```

Example:

```text
Batch 2: 7 novos terremotos gravados.
```

---

## 10. Check the stored data

```bash
docker compose exec postgres psql -U earthquake_user -d earthquakes
```

Example query:

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

The pipeline prevents duplicate earthquakes in multiple layers.

Spark first removes duplicate `event_id` values inside each batch.

It then compares incoming IDs against records already stored in PostgreSQL using a `LEFT ANTI JOIN`.

The PostgreSQL table also defines:

```sql
event_id PRIMARY KEY
```

which provides an additional uniqueness constraint.

---

## Streaming Checkpoints

Spark Structured Streaming stores persistent checkpoint data at:

```text
/opt/spark/checkpoints/earthquakes
```

This path is mapped to:

```text
./checkpoints
```

The checkpoint allows Spark to resume from the previously processed Kafka offsets after a restart instead of reading the entire stream again.

The checkpoint directory is ignored by Git.

---

## Tests

The project includes automated tests using `pytest`.

Run the test suite with:

```bash
python -m pytest
```

Current test coverage includes the transformation of raw USGS earthquake data into the standardized event format used by the pipeline.

Example:

```text
tests/test_producer.py . [100%]

1 passed
```

---

## Current Status

### Completed

- [x] USGS API ingestion
- [x] Python Kafka producer
- [x] Apache Kafka
- [x] Spark Structured Streaming
- [x] JSON parsing and transformation
- [x] PostgreSQL storage
- [x] Duplicate handling
- [x] Persistent Spark checkpoints
- [x] Dockerized infrastructure
- [x] Environment variables
- [x] Power BI dashboard
- [x] Automated test
- [x] Architecture diagram
- [x] Project screenshots
- [x] Git / GitHub repository

---

## Future Improvements

Possible future improvements include:

- PostgreSQL UPSERT using `ON CONFLICT`
- Kafka persistence
- Container health checks
- Automated Kafka topic creation
- Additional unit and integration tests
- Data quality validation
- Monitoring and observability
- CI/CD pipeline
- Cloud deployment

---

## Author

**Lucas**

GitHub: [@Lukao3777](https://github.com/Lukao3777)

---

# Português 🇧🇷

## Sobre o projeto

Este projeto é um pipeline de Engenharia de Dados utilizando dados reais de terremotos disponibilizados pela USGS.

O objetivo foi construir uma arquitetura de streaming real, indo além de projetos tradicionais baseados apenas em arquivos CSV.

O fluxo principal é:

```text
USGS API
   ↓
Python
   ↓
Apache Kafka
   ↓
Apache Spark
   ↓
PostgreSQL
   ↓
Power BI
```

O Python coleta os dados recentes da USGS e publica os eventos no Kafka.

O Spark Structured Streaming consome essas mensagens, transforma os dados, identifica eventos duplicados e grava somente novos terremotos no PostgreSQL.

O projeto também utiliza checkpoints persistentes para manter o progresso do streaming após reinicializações.

Os dados processados são utilizados em um dashboard criado no Power BI.

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
- Pytest
- Git / GitHub

---

## Funcionalidades implementadas

- Ingestão de dados reais da USGS
- Producer em Python
- Comunicação Python → Kafka
- Streaming Kafka → Spark
- Transformação dos dados com Spark
- Persistência Spark → PostgreSQL
- Tratamento de eventos duplicados
- Checkpoint persistente
- Configuração por variáveis de ambiente
- Infraestrutura com Docker Compose
- Dashboard no Power BI
- Testes automatizados
- Diagrama da arquitetura
- Documentação no GitHub

---

## Dashboard

O dashboard apresenta informações como:

- quantidade total de terremotos
- magnitude média
- maior magnitude registrada
- profundidade média
- atividade dos terremotos ao longo do tempo
- distribuição de magnitude
- localização geográfica dos eventos
- terremotos mais recentes

---

## Objetivo

O objetivo principal deste projeto é praticar conceitos reais de Engenharia de Dados, incluindo:

- streaming de eventos
- Apache Kafka
- Spark Structured Streaming
- processamento em micro-batches
- persistência em PostgreSQL
- deduplicação
- idempotência
- checkpoints
- Docker
- integração entre diferentes serviços
- visualização de dados com Power BI

---

## Status

✅ **Pipeline concluído e funcionando de ponta a ponta.**

```text
USGS → Python → Kafka → Spark → PostgreSQL → Power BI
```

O projeto continua aberto para futuras melhorias de infraestrutura, testes, observabilidade e deployment em cloud.