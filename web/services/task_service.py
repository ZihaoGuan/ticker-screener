"""Task orchestration service for managing screener runs."""
import redis
import os
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

try:
    redis_client = redis.from_url(redis_url)
except Exception as e:
    logger.warning(f'Failed to connect to Redis: {e}')
    redis_client = None

LOCK_TTL = 7200  # 2 hours


class TaskService:
    """Service for managing task queue operations."""
    
    @staticmethod
    def acquire_lock(screener_type, user_id):
        """
        Acquire distributed lock to prevent duplicate concurrent runs.
        
        Args:
            screener_type: Type of screener
            user_id: User ID
        
        Returns:
            str: lock_key if acquired, None if already locked
        """
        if not redis_client:
            logger.warning('Redis client not available for lock acquisition')
            return None
        
        lock_key = f'lock:screener:{screener_type}:user:{user_id}'
        
        try:
            # Use Redis SET with NX (only if not exists) for atomic operation
            acquired = redis_client.set(
                lock_key,
                datetime.utcnow().isoformat(),
                ex=LOCK_TTL,
                nx=True
            )
            
            if acquired:
                logger.info(f'Acquired lock: {lock_key}')
                return lock_key
            else:
                logger.info(f'Lock already held: {lock_key}')
                return None
        except Exception as e:
            logger.error(f'Failed to acquire lock: {e}')
            return None
    
    @staticmethod
    def release_lock(lock_key):
        """
        Release the distributed lock.
        
        Args:
            lock_key: Lock key to release
        """
        if not redis_client:
            return
        
        try:
            redis_client.delete(lock_key)
            logger.info(f'Released lock: {lock_key}')
        except Exception as e:
            logger.error(f'Failed to release lock: {e}')
    
    @staticmethod
    def submit_screener_run(screener_type, user_id, **kwargs):
        """
        Submit a screener run with concurrency control.
        
        Args:
            screener_type: Type of screener
            user_id: User ID
            **kwargs: Additional screener parameters
        
        Returns:
            dict: Run metadata with task_id
        
        Raises:
            RuntimeError: If already running
        """
        from web.tasks.celery_app import app
        
        # Check and acquire lock
        lock_key = TaskService.acquire_lock(screener_type, user_id)
        if not lock_key:
            raise RuntimeError(
                f'{screener_type} is already running. Please wait for completion.'
            )
        
        # Create run record (placeholder)
        run_id = 1  # Would be actual DB insert
        
        try:
            # Submit task
            task = app.send_task(
                'web.tasks.screener_tasks.run_screener_task',
                args=[run_id, screener_type, user_id],
                kwargs=kwargs,
                queue='screeners',
                priority=5
            )
            
            return {
                'run_id': run_id,
                'task_id': task.id,
                'lock_key': lock_key
            }
        except Exception as e:
            logger.error(f'Failed to submit task: {e}')
            TaskService.release_lock(lock_key)
            raise
    
    @staticmethod
    def get_run_status(run_id):
        """
        Get current run status and progress.
        
        Args:
            run_id: Run ID
        
        Returns:
            dict: Run status and metadata
        """
        from web.tasks.celery_app import app
        
        # Placeholder: would query Run model
        # run = Run.get(run_id)
        # task = app.AsyncResult(run.task_id)
        
        return {
            'run_id': run_id,
            'screener_type': 'rs_new_high',
            'status': 'queued',
            'task_state': 'PENDING',
            'progress': None,
            'started_at': None,
            'completed_at': None,
            'error_message': None,
            'result_count': 0
        }
