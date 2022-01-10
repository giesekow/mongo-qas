from pymongo.collection import Collection
from typing import Type, Callable
from datetime import datetime, timedelta
from .misc import toOid
import importlib

class Job:
  def __init__(self, id, data, collection: Type[Collection], **kwargs):
    self.id = toOid(id)
    self.coll = collection
    self.payload = data
    self.kwargs = kwargs
    self.result = None
    self.logger = None
    self.channel = None
    self.verbosity = "error"

    if "logger" in kwargs:
      self.setLogger(kwargs["logger"])

    if "verbosity" in kwargs:
      self.setVerbosity(kwargs["verbosity"])

    if "channel" in kwargs:
      self.channel = kwargs["channel"]

  def data(self):
    return self.payload

  def set_result(self, result):
    self.result = result
    self.coll.update_one({"_id": self.id}, {"$set": {"result": self.result}})
    return True

  def complete(self, result=None):
    if result is None:
      result = self.result
    
    set_data = {"progress": 100, "inProgress": False, "done": True, "result": result, "completedAt": datetime.utcnow()}
    if "result_ttl" in self.kwargs:
      ttl = self.kwargs.get("result_ttl")
      if int(ttl) > 0:
        set_data["expireAt"] = datetime.utcnow() + timedelta(seconds=int(ttl))

    self.coll.update_one({"_id": self.id}, {"$set": set_data})

    
    if "on_success" in self.payload:
      payload = self.payload
      try:
        if not payload is None:
          callback = payload.get("on_success")
          if not callback is None:
            if str(callback).__contains__("."):
              mod_name, func_name = callback.rsplit(".", 1)
              mod = importlib.import_module(mod_name)
              func = getattr(mod, func_name)
              args = payload.get("args", [])
              kwargs = payload.get("kwargs", {})
              func(result, *args, **kwargs)
            elif callback in globals():
              func = globals()[callback]
              args = payload.get("args", [])
              kwargs = payload.get("kwargs", {})
              func(result, *args, **kwargs)

      except Exception as ex:
        self.log("error", error_message=f"Error: {ex}")

    self.log("completed", result=result)
    return True

  def error(self, msg=None):   
    set_data = {"inProgress": False, "error": True, "lastErrorAt": datetime.utcnow(), "errorMessage": msg}

    if "failure_ttl" in self.kwargs:
      ttl = self.kwargs.get("failure_ttl")
      if int(ttl) > 0:
        set_data["expireAt"] = datetime.utcnow() + timedelta(seconds=int(ttl))
    
    self.coll.update_one({"_id": self.id}, {"$inc": {"attempts": 1}, "$set": set_data})
    
    if "on_failure" in self.payload:
      payload = self.payload
      try:
        if not payload is None:
          callback = payload.get("on_failure")
          if not callback is None:
            if str(callback).__contains__("."):
              mod_name, func_name = callback.rsplit(".", 1)
              mod = importlib.import_module(mod_name)
              func = getattr(mod, func_name)
              args = payload.get("args", [])
              kwargs = payload.get("kwargs", {})
              func(msg, *args, **kwargs)
            elif callback in globals():
              func = globals()[callback]
              args = payload.get("args", [])
              kwargs = payload.get("kwargs", {})
              func(msg, *args, **kwargs)

      except Exception as ex:
        self.log("error", error_message=f"Error: {ex}")

    self.log("error", error_message=msg)
    return True

  def log(self, type, *args, **kwargs):
    if not self.logger is None:
      if self.verbosity.lower().find(type) >= 0:
        payload = self.payload
        _args = payload.get("args", [])
        _kwargs = payload.get("kwargs", {})
        _args = _args + list(args)
        _kwargs.update(kwargs)
        self.logger(type, *_args, **_kwargs)

  def progress(self, percent, msg=None):
    self.coll.update_one({"_id": self.id}, {"$set": {"progress": percent, "progressMessage": msg, "lastProgressAt": datetime.utcnow()}})
    self.log("progress", percent=percent, progress_message=msg)
    return True

  def setLogger(self, logger):
    self.logger = logger

  def setVerbosity(self, verbosity):
    self.verbosity = verbosity

  def release(self):
    self.coll.update_one({"_id": self.id}, {"$set": {"inProgress": False, "error": False, "done": False, "releasedAt": datetime.utcnow(), "attempts": 0}})
    return True