# Task Queue Architecture for Screener Runs

## Overview

This document describes the Celery + Redis + WebSocket task queue implementation for managing long-running screener tasks in the web application.

## Architecture

### Components

- **Web Backend (FastAPI)**: Acts as a gatekeeper, receives HTTP requests, submits tasks to Celery, returns task_id immediately without blocking
- **Celery Worker**: Executes Python screener scripts in background processes, fully separated from the web thread
- **Redis**: Message broker for Celery and Pub/Sub channel for real-time log streaming
- **WebSocket Service**: Listens to Redis Pub/Sub channels and pushes logs to frontend in real-time
- **PostgreSQL**: Stores run metadata, task logs, and distributed locks

## Database Schema

### runs table
Tracks screener execution runs initiated from the web app:
- `id`: Primary key
- `user_id`: User who initiated the run
- `screener_type`: Type of screener (e.g., 'rs_new_high', 'vcp', 'cup_handle')
- `status`: Current status ('queued', 'running', 'completed', 'failed')
- `task_id`: Celery task UUID
- `started_at`: Timestamp when execution began
- `completed_at`: Timestamp when execution finished
- `error_message`: Error details if failed
- `result_count`: Number of results found
- `created_at`: When record was created
- `updated_at`: Last update timestamp

### task_logs table
Real-time audit trail of all logs from screener execution:
- `id`: Primary key
- `run_id`: Foreign key to runs
- `message`: Log message content
- `level`: Log level (INFO, DEBUG, ERROR, WARNING)
- `created_at`: Timestamp of log entry

### task_locks table
Distributed lock mechanism for concurrency control:
- `id`: Primary key
- `screener_type`: Type of screener
- `user_id`: User running the screener
- `acquired_at`: When lock was acquired
- `expires_at`: TTL expiration time
- UNIQUE constraint on (screener_type, user_id)

## Key Safety Features

### 1. Script Concurrency Control (Race Conditions & Locking)

**Problem**: Multiple users (or same user) clicking execute simultaneously could spawn duplicate tasks

**Solution**: Distributed Redis lock with 2-hour TTL
```python
# User A acquires lock for 'rs_new_high' screener
lock_key = TaskService.acquire_lock('rs_new_high', user_id=1)

# User A's task runs...

# If User B tries same screener:
# TaskService.acquire_lock() returns None (lock already held)
# Raises RuntimeError: "rs_new_high is already running..."
```

### 2. Browser Crash via Log Flooding

**Problem**: High-frequency logging (thousands of lines/sec) overwhelms WebSocket and browser DOM

**Solution**: Multi-layer throttling
- **Backend**: Batch logs every 100ms or 10 lines (whichever comes first)
- **Frontend**: Keep only last 1,000 lines in terminal UI, discard older entries
- **WebSocket**: Message queue depth monitoring

### 3. Orphaned Processes (Zombie Processes)

**Problem**: User disconnects → WebSocket closes → subprocess keeps running forever → drains CPU/memory

**Solution**: Explicit cleanup on disconnect
```python
@router.websocket('/ws/logs/{run_id}')
async def websocket_logs(websocket: WebSocket, run_id: int):
    try:
        # Listen to Redis Pub/Sub...
    except Exception as e:
        pass
    finally:
        # Kill subprocess if still running
        if subprocess_handle:
            subprocess_handle.terminate()
            try:
                subprocess_handle.wait(timeout=5)
            except subprocess.TimeoutExpired:
                subprocess_handle.kill()
        redis.close()
        await websocket.close()
```

### 4. Graceful Timeout Management

- **Soft limit**: 7000s (1h 56m) - triggers SoftTimeLimitExceeded exception
- **Hard limit**: 7200s (2h) - Celery forcefully terminates task
- **Subprocess timeout**: 7200s explicit wait timeout
- Allows task to catch soft limit, cleanup resources, then fail gracefully

### 5. Automatic Retry & Error Handling

- Failed tasks retry once after 60s delay
- Full exception logging and broadcast to frontend via WebSocket
- Error message persisted to database

## Deployment

### Local Development

```bash
# Start services
docker-compose -f deploy/docker-compose.yml up

# Or run components separately:
# Terminal 1: Web server
uvicorn web.app:app --reload

# Terminal 2: Celery worker
celery -A web.tasks.celery_app worker -l info -c 1 --queues screeners,default

# Terminal 3: (Optional) Celery beat for scheduled tasks
celery -A web.tasks.celery_app beat -l info
```

### Production Deployment

See `deploy/docker-compose.yml` for full stack with:
- PostgreSQL database
- Redis cache and message broker
- FastAPI web server
- Celery worker (1 concurrent task)
- Celery beat scheduler (optional)
- Caddy reverse proxy with automatic HTTPS

## API Endpoints

### Submit Screener Run
```http
POST /api/runs/submit
Content-Type: application/json

{
  "screener_type": "rs_new_high",
  "user_id": 1,
  "limit": 25,
  "tickers": ["AAPL", "MSFT"],
  "start_date": "2024-01-01"
}

Response:
{
  "run_id": 123,
  "task_id": "abc-def-123",
  "lock_key": "lock:screener:rs_new_high:user:1"
}
```

### Get Run Status
```http
GET /api/runs/status/123

Response:
{
  "run_id": 123,
  "screener_type": "rs_new_high",
  "status": "running",
  "task_state": "PROGRESS",
  "progress": {"status": "running"},
  "started_at": "2026-05-28T10:30:00Z",
  "completed_at": null,
  "error_message": null,
  "result_count": 0
}
```

### Get Run History
```http
GET /api/runs/history?user_id=1&limit=20

Response:
[
  {
    "id": 123,
    "screener_type": "rs_new_high",
    "status": "completed",
    "created_at": "2026-05-28T10:30:00Z",
    "result_count": 45
  },
  ...
]
```

### Get Task Logs
```http
GET /api/runs/logs/123?limit=1000

Response:
{
  "logs": [
    {
      "timestamp": "2026-05-28T10:30:05Z",
      "level": "INFO",
      "message": "Starting rs_new_high screener"
    },
    ...
  ]
}
```

### WebSocket for Real-Time Logs
```javascript
// Client-side
const ws = new WebSocket('ws://localhost:8000/api/runs/ws/logs/123');

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  
  if (message.type === 'history') {
    // Received log history (first message)
    console.log('History:', message.logs);
  } else if (message.type === 'log') {
    // Received new log entry (real-time)
    console.log(`[${message.data.level}] ${message.data.message}`);
  }
};
```

## Configuration

### Environment Variables

```bash
# Redis
REDIS_URL=redis://localhost:6379/0
REDIS_RESULT_BACKEND=redis://localhost:6379/1

# Celery task timeout (seconds)
CELERY_TASK_SOFT_TIME_LIMIT=7000
CELERY_TASK_HARD_TIME_LIMIT=7200

# Log batching
LOG_BATCH_SIZE=10              # Lines before flush
LOG_BATCH_TIMEOUT_MS=100       # Time before flush (ms)

# Frontend terminal
FRONTEND_LOG_MAX_LINES=1000    # Max lines to keep
```

## Monitoring

### Celery Flower (Optional)
```bash
pip install flower
celery -A web.tasks.celery_app flower --port=5555
# Then visit http://localhost:5555
```

### Check Celery Worker Status
```bash
celery -A web.tasks.celery_app inspect active
celery -A web.tasks.celery_app inspect stats
```

### Redis CLI
```bash
redis-cli
> KEYS logs:run:*          # All log channels
> DBSIZE                   # Total keys
> FLUSHDB                  # Clear database
```

## Troubleshooting

### Celery Worker Not Processing Tasks

```bash
# Check if worker is running
ps aux | grep celery

# Check Redis connection
redis-cli ping

# Check Celery worker logs
celery -A web.tasks.celery_app inspect active_queues
```

### High Memory Usage

- Reduce `worker_prefetch_multiplier` (already set to 1)
- Increase task timeout so completed tasks clear faster
- Monitor with `redis-cli INFO memory`

### WebSocket Disconnects

- Check browser console for errors
- Verify Caddy/nginx is not blocking WebSocket upgrades
- Ensure `WEBSOCKET_ALLOWED_HOSTS` includes your domain

### Orphaned Processes

```bash
# Find zombie processes
ps aux | grep defunct

# Or Python processes consuming CPU
ps aux | grep python | grep run_screener
```

## Future Enhancements

1. **Scheduled Screeners**: Use Celery Beat for daily/weekly automated runs
2. **Result Persistence**: Store screener outputs in database instead of artifacts only
3. **Priority Queue**: Route screeners by user tier or importance
4. **Rate Limiting**: Prevent user from submitting too many concurrent tasks
5. **Notifications**: Email or Slack alerts when run completes
6. **Retry Strategy**: Exponential backoff instead of fixed delay
