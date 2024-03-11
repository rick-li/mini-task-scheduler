
from typing import Dict
from log import logger
from threading import Thread
import threading
import time
import socket
from task import Task, complete_task, get_pending_tasks, update_task

host_name = socket.gethostname()

sleep_time = 1
scheduler = None

def start_workers(count = 2):
    global workers, scheduler
    
    for i in range(count):
        worker_id = host_name + '-worker-' + str(i)
        logger.info(f"Starting worker {worker_id}")
        worker = Worker(worker_id)
        workers[worker_id] = worker
        worker.start()

    scheduler = TScheduler()
    scheduler.start()


def stop():
    global scheduler, workers
    if scheduler:
        scheduler.stop()
        scheduler = None

    for worker_key in workers.keys():
        if workers[worker_key] != None :
            workers[worker_key].stop()
        workers[worker_key] = None

class TScheduler(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def is_stopped(self):
        return self._stop.isSet()
    
    def run(self):
        logger.info(f"Scheduler started.")
        
        while not self.is_stopped():
            logger.info(f"Scheduler running.")
            tasks = get_pending_tasks()
            logger.info(f"Found tasks: {tasks}")
            for task in tasks:
                logger.info(f"Found task: {task}")
                for worker_key in workers.keys():
                    if workers[worker_key] != None and workers[worker_key].task == None:
                        workers[worker_key].submit_task(task)
                        break
            time.sleep(3)

class Worker(Thread):
    task = None
    
    def __init__(self, worker_id: str):
        Thread.__init__(self)
        self.worker_id = worker_id
        self._stop = threading.Event()

    def submit_task(self, task: Task):
        self.task = task
        self.task.worker = self.worker_id

    def stop(self):
        self._stop.set()

    def is_stopped(self):
        return self._stop.isSet()
    
    def run(self):
        logger.info(f"Worker {self.worker_id} started.")
        while not self.is_stopped():
            if(self.task):
                logger.info(f"Worker {self.worker_id} executing task {self.task}")
                update_task(self.task, 0)
                
                #TODO do the job
                time.sleep(5)
                
                complete_task(self.task)
                logger.info(f"Worker {self.worker_id} completed task {self.task}")

                self.task = None
            time.sleep(sleep_time)

workers : Dict[str, Worker] = {}


def get_scheduler():
    return scheduler

def get_workers():
    return workers


