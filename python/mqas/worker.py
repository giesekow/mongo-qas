from datetime import datetime, timedelta

from bson.objectid import ObjectId
from .queue import Queue
from .job import Job
from .utils import executionCode
from typing import Dict, Type, Union, Tuple, Callable
from time import sleep
import importlib
import subprocess
import threading
import sys, json, os, traceback
from bson import json_util
from .misc import getSystemInfo

class WorkerThread(threading.Thread):
  def __init__(self, function_name, executable=None, args=[], kwargs={}, stdout=None, modulePaths=[], job=None):
    threading.Thread.__init__(self)
    self.function_name=function_name
    self.executable=executable
    self.args=args
    self.kwargs=kwargs
    self.stdout=stdout
    self.modulePaths=modulePaths
    self.job=job
    self.output=None

  def run(self):
    function_name=self.function_name
    executable=self.executable
    args=self.args
    kwargs=self.kwargs
    stdout=self.stdout
    modulePaths=self.modulePaths

    if executable is None:
      executable = sys.executable

    if modulePaths is None:
      modulePaths = []

    data = {"function_name": function_name, "args": args, "kwargs": kwargs, "stdout": stdout, "modules": modulePaths + [os.getcwd(),]}
    result = subprocess.run([executable, "-c", executionCode], stdout=subprocess.PIPE, input=str.encode(json.dumps(data, default=json_util.default)))
    out = result.stdout.decode().split("\n")
    if len(out) > 0:
      output = json.loads(out[-1], object_hook=json_util.object_hook)
      self.output = output
  
  def get_output(self):
    return self.output

  def get_job(self):
    return self.job

class Worker:

  def __init__(self, queues: Union[Tuple[Queue,...], Type[Queue]], channel: Union[Tuple[str,...], str]=None, heart_beat: int = 1, verbosity: str = "error", logger: Union[str, Callable] = None, executables: Dict = None, logFile: str = None, modulePaths=[]) -> None:
    
    if isinstance(queues, tuple) or isinstance(queues, list):
      self.queues = queues
    elif isinstance(queues, Queue):
      self.queues = tuple([queues])

    if isinstance(channel, tuple) or isinstance(channel, list):
      self._channels = channel
    elif isinstance(channel, str):
      self._channels = [channel]
    else:
      self._channels = None

    self._working = False
    self._heart_beat = heart_beat
    self._running = False

    self._verbosity = verbosity
    self.setLogger(logger)

    self.logFile = None
    if not logFile is None:
      self.logFile = logFile

    self.modulePaths = modulePaths

    if not executables is None and isinstance(executables, dict):
      self.executables = executables
    else:
      self.executables = {}

    self.worker_info = getSystemInfo()

    self.worker_id = None
    self.status_update_frequency = 10
    self.current_status_update_timeout = 10
    self.jobs_completed = 0
    self.jobs_failed = 0
    self.last_error_message = None

    self.thread = None
    
  def setLogger(self, logger: Union[str, Callable]):
    if callable(logger):
      self._logger = logger
    elif isinstance(logger, str):
      callback = str(logger)
      if str(callback).__contains__("."):
        mod_name, func_name = callback.rsplit(".", 1)
        mod = importlib.import_module(mod_name)
        func = getattr(mod, func_name)
        self._logger = func
      elif callback in globals():
        func = globals()[callback]
        self._logger = func
    else:
      self._logger = None

  def setVerbosity(self, verbosity: str):
    self.verbosity = verbosity

  def start(self):
    self._running = True
    self._run()

  def beat_heart(self):
    self.current_status_update_timeout += 1
    if self.current_status_update_timeout >= self.status_update_frequency:
      self.current_status_update_timeout = 0
      if len(self.queues) > 0:
        c_dt = datetime.utcnow()
        data = {
          "status": "working" if self._working else "waiting",
          "expireAt": datetime.utcnow() + timedelta(seconds=self._heart_beat + 10 + self.status_update_frequency),
          "queues": [q.channel for q in self.queues],
          "info": self.worker_info,
          "jobs_completed": self.jobs_completed,
          "jobs_failed": self.jobs_failed,
          "last_error_message": self.last_error_message,
          "is_worker": True
        }

        coll = self.queues[0].collection
        try:  
          if self.worker_id is None:
            data["createdAt"] = c_dt
            data["updatedAt"] = c_dt
            res = coll.insert_one(data)
            self.worker_id = res.inserted_id
          else:
            data["updatedAt"] = c_dt
            res = coll.find_one_and_update({"_id": ObjectId(self.worker_id)}, {"$set": data}, projection={"_id": True})
            if res is None:
              data["createdAt"] = c_dt
              data["updatedAt"] = c_dt
              res = coll.insert_one(data)
              self.worker_id = res.inserted_id
        except Exception as e:
          print(f"Worker status update error: {e}")
          
  def stop(self):
    self._running = False

  def _work(self):
    self._working = True
    job: Type[Job] = None

    for queue in self.queues:
      if self._channels is None:
        job = queue.dequeue()
        if not job is None:
          break
      else:
        for channel in self._channels:
          job = queue.dequeue(channel)
          if not job is None:
            break
        if not job is None:
          break

    if not job is None:
      self._run_job(job)
      # self._working = False
    else:
      self._working = False

  def _on_output(self, job, output):
    payload = job.payload
    callback = payload.get("function_name")
    args = payload.get("args", [])
    kwargs = payload.get("kwargs", {})

    if not output is None:
      if "result" in output:
        job.complete(output["result"])
        self.jobs_completed += 1
      if "error" in output:
        print(output["error"])
        job.error(output["error"])
        self.jobs_failed += 1
        self.last_error_message = output["error"] if isinstance(output["error"], dict) else dict(message=output["error"], trace="")
        self.last_error_message["errorAt"] = datetime.utcnow()
        self.last_error_message["callback"] = str(callback)
        self.last_error_message["jobId"] = job.id
  
  def _run_job(self, job: Type[Job]):
    payload = job.payload
    job.setVerbosity(self._verbosity)
    job.setLogger(self._logger)
    callback = None
    args = None
    kwargs = None
    try:
      if not payload is None:
        callback = payload.get("function_name")

        if not callback is None:
          args = payload.get("args", [])
          kwargs = payload.get("kwargs", {})
          executable = None
          channel = job.channel

          if not channel is None:
            if channel in self.executables:
              executable = self.executables[channel]

          self.thread = WorkerThread(callback, args=args, kwargs=kwargs, executable=executable, stdout=self.logFile, modulePaths=self.modulePaths, job=job)
          self.thread.start()

        else:
          job.error(f"Error: no callback funtion specified for job!")
          print(f"Error: no callback funtion specified for job!")

    except Exception as ex:
      errtrace = traceback.format_exc()
      job.error({"message": f"Error: {ex}", "trace": str(errtrace)})
      self.jobs_failed += 1
      self.last_error_message = dict(message=f"Error: {ex}", trace=str(errtrace))
      self.last_error_message["errorAt"] = datetime.utcnow()
      self.last_error_message["callback"] = callback
      self.last_error_message["jobId"] = job.id
      print(f"Error: {ex}")

  def _run(self):
    while True:
      if not self._running:
        break

      if not self._working:
        self._work()

      if not self.thread is None:
        out = self.thread.get_output()
        if not out is None:
          job = self.thread.get_job()
          self._on_output(job, out)
          self.thread.join()
          self.thread = None
          self._working = False

      self.beat_heart()
      sleep(self._heart_beat)