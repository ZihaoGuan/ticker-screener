"""Screener task definitions for Celery."""
import json
import logging
import subprocess
import os
from datetime import datetime
from celery import shared_task, current_task
from celery.exceptions import SoftTimeLimitExceeded

logger = logging.getLogger(__name__)


class LogCapture:
    """Capture subprocess logs and broadcast via Redis Pub/Sub."""
    
    def __init__(self, run_id, broadcast_fn):
        self.run_id = run_id
        self.broadcast_fn = broadcast_fn
        self.line_buffer = []
        self.last_flush = datetime.now()

    def write(self, message):
        """Write message and buffer for batching."""
        if message and message.strip():
            self.line_buffer.append(message.strip())
            # Batch logs every 10 lines or 100ms
            if len(self.line_buffer) >= 10 or \
               (datetime.now() - self.last_flush).total_seconds() > 0.1:
                self.flush()

    def flush(self):
        """Flush buffered logs."""
        if self.line_buffer:
            for msg in self.line_buffer:
                self.broadcast_fn(msg, 'INFO')
            self.line_buffer = []
            self.last_flush = datetime.now()

    def close(self):
        """Close and flush remaining logs."""
        self.flush()


# Import here to avoid circular imports
from web.tasks.celery_app import app


@shared_task(
    bind=True,
    queue='screeners',
    priority=5,
    max_retries=1,
    soft_time_limit=7000
)
def run_screener_task(self, run_id, screener_type, user_id, **kwargs):
    """
    Execute a screener in the background with log streaming.
    
    Args:
        run_id: Database run record ID
        screener_type: Type of screener (e.g., 'rs_new_high', 'vcp', 'cup_handle')
        user_id: User ID for permission context
        **kwargs: Additional screener-specific parameters
    
    Returns:
        dict: Task result with status and metadata
    """
    from web.services.log_broadcaster import broadcast_log
    from web.services.task_service import TaskService
    
    # Placeholder for Run model - will be implemented
    run = None
    log_channel = f'logs:run:{run_id}'
    subprocess_handle = None
    lock_key = None
    
    try:
        # Create broadcast function
        def broadcast(msg, level):
            broadcast_log(log_channel, msg, level)
        
        # Update run status (placeholder)
        broadcast(f'Starting {screener_type} screener for user {user_id}', 'INFO')
        
        # Map screener type to script
        script_map = {
            'rs_new_high': 'scripts/run_rs_screen.py',
            'weekly_rs': 'scripts/run_weekly_rs_screen.py',
            'vcp': 'scripts/run_vcp_screen.py',
            'cup_handle': 'scripts/run_cup_handle_screen.py',
            'peg': 'scripts/run_peg_screen.py',
        }
        
        script_path = script_map.get(screener_type)
        if not script_path:
            raise ValueError(f'Unknown screener type: {screener_type}')
        
        # Build command
        cmd = ['python3', script_path]
        
        # Add optional parameters
        if kwargs.get('limit'):
            cmd.extend(['--limit', str(kwargs['limit'])])
        if kwargs.get('tickers'):
            cmd.extend(['--tickers', ' '.join(kwargs['tickers'])])
        if kwargs.get('start_date'):
            cmd.extend(['--start-date', kwargs['start_date']])
        
        broadcast(f'Command: {" ".join(cmd)}', 'DEBUG')
        
        # Execute with log capture
        log_capture = LogCapture(run_id, broadcast)
        subprocess_handle = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env={**os.environ, 'PYTHONUNBUFFERED': '1'}
        )
        
        # Stream output
        for line in subprocess_handle.stdout:
            log_capture.write(line)
            current_task.update_state(state='PROGRESS', meta={'status': 'running'})
        
        log_capture.close()
        
        # Wait for completion
        return_code = subprocess_handle.wait(timeout=7200)
        subprocess_handle = None
        
        if return_code != 0:
            raise RuntimeError(f'Screener script exited with code {return_code}')
        
        broadcast(f'Screener completed successfully', 'INFO')
        
        return {
            'status': 'success',
            'run_id': run_id,
            'result_count': 0  # Placeholder
        }
    
    except SoftTimeLimitExceeded:
        logger.error(f'Task {self.request.id} exceeded soft time limit')
        broadcast('Task exceeded time limit', 'ERROR')
        
        # Cleanup subprocess
        if subprocess_handle:
            subprocess_handle.terminate()
            try:
                subprocess_handle.wait(timeout=5)
            except subprocess.TimeoutExpired:
                subprocess_handle.kill()
        
        return {'status': 'failed', 'error': 'Time limit exceeded'}
    
    except Exception as e:
        logger.exception(f'Screener task failed: {str(e)}')
        broadcast(f'Error: {str(e)}', 'ERROR')
        
        # Cleanup subprocess
        if subprocess_handle:
            subprocess_handle.terminate()
            try:
                subprocess_handle.wait(timeout=5)
            except subprocess.TimeoutExpired:
                subprocess_handle.kill()
        
        if self.request.retries < self.max_retries:
            return self.retry(exc=e, countdown=60)
        
        return {'status': 'failed', 'error': str(e)}
