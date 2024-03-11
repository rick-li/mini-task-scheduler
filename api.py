from fastapi import FastAPI
import uvicorn
from task import Task, create_task
from worker import get_scheduler, start_workers, stop, get_workers
from log import logger

app = FastAPI()

@app.post("/task")
def _create_task(task: Task):
    logger.info(task.model_dump_json())
    try:
        create_task(task)
    except Exception as e:
        return {"status": "error", "message": str(e)}
    return {"status": "success", "data": task.dict()}

@app.get("/status")
def _status():
    scheduler = get_scheduler()
    
    workers = get_workers()
    scheduler_status = "running" if scheduler != None and scheduler.is_alive() else "stopped"
    logger.info(f"scheduler stopped ")
    worker_status = "running" if len(workers.items()) > 0 and any(worker != None and worker.is_alive() for worker in workers.values()) else "stopped"
    return {"scheduler": scheduler_status, "workers": worker_status}
    # return {"scheduler": "stopped", "workers": worker_status}


# Test apis
@app.get("/start")
def _start():
    start_workers()
    return "ok"

@app.get("/stop")
def _stop():
    stop()
    return "ok"


if __name__ == "__main__":
    uvicorn.run('api:app', host="0.0.0.0", port=8000, reload=True)