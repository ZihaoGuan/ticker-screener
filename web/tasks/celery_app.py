"""Celery app configuration for task queue."""
from celery import Celery
from kombu import Exchange, Queue
import os

broker_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
result_backend = os.getenv('REDIS_RESULT_BACKEND', 'redis://localhost:6379/1')

app = Celery(
    'ticker_screener',
    broker=broker_url,
    backend=result_backend,
    include=['web.tasks.screener_tasks']
)

# Task configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=7200,  # 2 hours hard limit
    task_soft_time_limit=7000,  # 1h 56m soft limit
    worker_prefetch_multiplier=1,  # Only fetch one task at a time
    task_acks_late=True,  # Ack after completion
    task_reject_on_worker_lost=True,
    result_expires=3600,  # Results expire after 1 hour
)

# Prioritize screener tasks
app.conf.task_queues = (
    Queue('screeners', Exchange('screeners'), routing_key='screeners.#', queue_arguments={'x-max-priority': 10}),
    Queue('default', routing_key='default'),
)

app.conf.task_default_queue = 'default'
app.conf.task_default_exchange = 'tasks'
app.conf.task_default_routing_key = 'default'
