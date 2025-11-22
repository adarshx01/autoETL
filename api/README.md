# AutoETL REST API

This directory contains the FastAPI REST API server for the AutoETL system.

## Files

### `main.py`
FastAPI application with all API endpoints:
- Health checks
- Database discovery (list tables, schemas, preview)
- ETL execution endpoints (Stage 1)
- Validation endpoints (Stage 2)
- Combined workflow endpoints
- Reports and metrics

### `models.py`
Pydantic models for request/response validation:
- Request models for each endpoint
- Response models with examples
- Status tracking models
- Metrics models

### `workflow_manager.py`
Orchestrates job execution:
- Job registration and tracking
- Async ETL execution
- Async validation execution
- Combined workflow orchestration
- Job status management

## Running the API

### Quick Start
```bash
# From project root
./start_server.sh
```

### Manual Start
```bash
# Activate virtual environment
source venv/bin/activate

# Start server
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

### Access API Documentation
- Interactive docs: http://localhost:8000/docs
- OpenAPI spec: http://localhost:8000/openapi.json

## API Endpoints

### Health
- `GET /` - Basic status
- `GET /health` - Detailed health check

### Database Discovery
- `GET /api/databases/source/tables` - List source tables
- `GET /api/databases/target/tables` - List target tables
- `GET /api/databases/{db}/tables/{table}/schema` - Get schema
- `POST /api/databases/source/tables/{table}/preview` - Preview data

### ETL (Stage 1)
- `POST /api/etl/execute` - Start ETL job
- `GET /api/etl/status/{job_id}` - Get job status
- `GET /api/etl/jobs` - List all ETL jobs

### Validation (Stage 2)
- `POST /api/validate/execute` - Start validation
- `GET /api/validate/status/{job_id}` - Get status
- `GET /api/validate/jobs` - List validation jobs

### Workflow
- `POST /api/workflow/execute` - Start combined workflow
- `GET /api/workflow/status/{workflow_id}` - Get workflow status

### Reports & Metrics
- `GET /api/reports/{job_id}` - Download HTML report
- `GET /api/reports/{job_id}/results` - Get JSON results
- `GET /api/metrics/etl` - ETL metrics
- `GET /api/metrics/validation` - Validation metrics

### NLP
- `POST /api/parse-rule` - Parse natural language business rule

## Example Usage

### Parse Business Rule
```bash
curl -X POST http://localhost:8000/api/parse-rule \
  -H "Content-Type: application/json" \
  -d '{
    "business_rule": "Calculate total orders for each customer from the last year"
  }'
```

### Start ETL Job
```bash
curl -X POST http://localhost:8000/api/etl/execute \
  -H "Content-Type: application/json" \
  -d '{
    "source_table": "customers",
    "target_table": "customer_summary",
    "business_rule": "Calculate total orders and revenue per customer",
    "batch_size": 10000,
    "use_dask": false,
    "user_id": "test_user"
  }'
```

### Check Job Status
```bash
curl http://localhost:8000/api/etl/status/JOB_ID
```

### List Available Tables
```bash
curl http://localhost:8000/api/databases/source/tables
```

## Development

### Adding New Endpoints
1. Add endpoint function in `main.py`
2. Create request/response models in `models.py`
3. Update workflow manager if needed
4. Test with API docs at http://localhost:8000/docs

### Error Handling
All endpoints use HTTPException for errors:
```python
raise HTTPException(
    status_code=500,
    detail="Error message here"
)
```

### Background Tasks
Long-running operations use FastAPI BackgroundTasks:
```python
@app.post("/api/etl/execute")
async def execute_etl(
    request: ETLExecutionRequest,
    background_tasks: BackgroundTasks
):
    job_id = workflow_manager.register_etl_job(...)
    background_tasks.add_task(
        workflow_manager.execute_etl_job,
        job_id
    )
    return {"job_id": job_id}
```

## Configuration

API server configuration via environment variables:
- `SOURCE_DB_*` - Source database connection
- `TARGET_DB_*` - Target database connection
- `AI_PROVIDER` - OpenAI or Gemini
- `OPENAI_API_KEY` / `GOOGLE_API_KEY` - AI provider keys
- `REDIS_*` - Redis configuration (optional)

See `.env.example` for full configuration options.

## Testing

### Test Health Endpoint
```bash
curl http://localhost:8000/health
```

### Test with API Docs
Visit http://localhost:8000/docs and use the interactive interface to test all endpoints.

## Production Considerations

### Security
- [ ] Add authentication (JWT tokens)
- [ ] Restrict CORS origins
- [ ] Enable rate limiting
- [ ] Use HTTPS/TLS
- [ ] Validate all inputs

### Performance
- [ ] Use connection pooling (already configured)
- [ ] Enable Redis caching
- [ ] Add request timeout limits
- [ ] Implement job queue (Celery/RabbitMQ)

### Monitoring
- [ ] Add Prometheus metrics
- [ ] Enable structured logging
- [ ] Track API response times
- [ ] Monitor database connections

### Deployment
- [ ] Use process manager (systemd, supervisor)
- [ ] Set up reverse proxy (nginx)
- [ ] Configure SSL certificates
- [ ] Enable log rotation
- [ ] Set up health checks

## Architecture

```
Web Interface (index.html)
    ↓ HTTP REST calls
FastAPI Server (main.py)
    ↓ Job orchestration
Workflow Manager (workflow_manager.py)
    ↓ Async execution
ETL Engine + Validation Agent
    ↓ Database operations
PostgreSQL/MySQL/SQL Server
```
