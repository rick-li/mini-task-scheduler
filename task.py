from typing import Optional
from pydantic import BaseModel
from db import get_conn
from log import logger

class Task(BaseModel):
    id: Optional[int] = None
    user_id: str
    file_name: str
    status: str
    total_chunks: int
    chunks_processed: Optional[int] = 0
    worker: Optional[str] = None
    

def update_task(task: Task, chunks_processed: int) -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            sql = """
                UPDATE tasks SET status = 'processing', chunks_processed = %s, worker = %s, last_updated = CURRENT_TIMESTAMP WHERE id = %s
            """
            # Prepare the values
            values = (chunks_processed, task.worker, task.id)
            # Execute the SQL command
            cur.execute(sql, values)
            conn.commit()
            return True
    except Exception as e:
        print("An error occurred: ", e)
        conn.rollback()
        return False
    finally:
        conn.close()

def complete_task(task: Task) -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            sql = """
                UPDATE tasks SET status = 'done', chunks_processed = %s, last_updated = CURRENT_TIMESTAMP WHERE id = %s
            """
            # Prepare the values
            values = (task.chunks_processed, task.id)
            # Execute the SQL command
            cur.execute(sql, values)
            conn.commit()
            return True
    except Exception as e:
        print("An error occurred: ", e)
        conn.rollback()
        return False
    finally:
        conn.close()

def create_task(task: Task):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO tasks (user_id, file_name, status, total_chunks, chunks_processed, worker, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
            # Prepare the values
            values = (task.user_id, task.file_name, task.status, task.total_chunks, 0, None)
            # Execute the SQL command
            cur.execute(sql, values)
            conn.commit()
            
    except Exception as e:
        print("An error occurred: ", e)
        conn.rollback()
        raise
        
    finally:
        conn.close()

def get_pending_tasks() -> list[Task]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, user_id, file_name, status, total_chunks, chunks_processed, last_updated FROM tasks WHERE status = 'pending' ORDER BY last_updated ASC")
            rows = cur.fetchall()
            tasks = []
            for row in rows:
                task = Task(
                    id=row[0],
                    user_id=row[1],
                    file_name=row[2],
                    status=row[3],
                    total_chunks=row[4],
                    chunks_processed=row[5]
                )
                tasks.append(task)
            return tasks
    except Exception as e:
        logger.info("An error occurred: ", e)
        return []
    finally:
        conn.close()
    