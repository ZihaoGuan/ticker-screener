"""Log broadcasting service for real-time log streaming."""
import redis
import json
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

try:
    redis_client = redis.from_url(redis_url)
except Exception as e:
    logger.warning(f'Failed to connect to Redis: {e}')
    redis_client = None


def broadcast_log(channel, message, level='INFO'):
    """
    Broadcast log to Redis Pub/Sub and persist to DB.
    
    Args:
        channel: Redis channel (e.g., 'logs:run:123')
        message: Log message
        level: Log level (INFO, DEBUG, ERROR, WARNING)
    """
    log_entry = {
        'timestamp': datetime.utcnow().isoformat(),
        'message': message,
        'level': level
    }
    
    # Publish to Redis Pub/Sub
    if redis_client:
        try:
            redis_client.publish(channel, json.dumps(log_entry))
        except Exception as e:
            logger.error(f'Failed to publish to Redis: {e}')
    
    # Persist to database (extract run_id from channel)
    if channel.startswith('logs:run:'):
        try:
            run_id = int(channel.split(':')[-1])
            # Import here to avoid circular imports
            # from web.models.task_log import TaskLog
            # TaskLog.create(run_id=run_id, message=message, level=level)
            logger.debug(f'Would persist log: run_id={run_id}, message={message[:100]}')
        except Exception as e:
            logger.error(f'Failed to persist log: {e}')


def get_log_history(run_id, limit=1000):
    """
    Get recent log history for a run.
    
    Args:
        run_id: Run ID
        limit: Maximum number of logs to return
    
    Returns:
        list: List of log entries
    """
    # Placeholder: would query TaskLog model
    # from web.models.task_log import TaskLog
    # logs = TaskLog.query(run_id=run_id).order_by('-created_at').limit(limit)
    # return [{'message': log.message, 'level': log.level, 'timestamp': log.created_at.isoformat()} 
    #         for log in logs]
    return []
