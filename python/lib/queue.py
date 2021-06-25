from pymongo import DESCENDING, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo import MongoClient
from pymongo.results import InsertOneResult
from typing import Union, Callable, Optional, Type
from datetime import datetime
from .job import Job
from .misc import parse_time_to_seconds, toOid

class Queue:
  def __init__(self, connection: Union[str, Type[Collection], Type[Database], Type[MongoClient]], consumerId: str = "default-customer-id", lang: str = "python", channel: str = "default", priority: int = 0, job_timeout: Union[str, int, None]='1h', result_ttl: Union[str,int,None]=500, ttl: Union[str,int,None]=None, failure_ttl: Union[str,int]='1y', max_attempts: int=1, db_name: Optional[str]="jobs", col_name: Optional[str]="jobs") -> None:
    
    if isinstance(connection, MongoClient):
      db = connection[db_name]
      self.collection = db[col_name]
    elif isinstance(connection, Database):
      self.collection = connection[col_name]
    elif isinstance(connection, Collection):
      self.collection = connection
    elif isinstance(connection, str):
      self.collection = MongoClient(connection)[db_name][col_name]
    else:
      print("Connection object should be one of either a mongoclient, database or collection objects, or a string representing the connection url", "\n", "Current object type is", type(connection))
      self.collection = connection

    self.lang = lang
    self.consumerId = consumerId
    self.job_timeout = parse_time_to_seconds(job_timeout) if not job_timeout is None else job_timeout
    self.max_attempts = max_attempts
    self.channel = channel
    self.result_ttl = parse_time_to_seconds(result_ttl) if not result_ttl is None else result_ttl
    self.ttl = ttl = parse_time_to_seconds(ttl) if not ttl is None else ttl
    self.failure_ttl = parse_time_to_seconds(failure_ttl) if not failure_ttl is None else failure_ttl
    self.priority = priority

  def enqueue(self, function_name: Union[Callable,str], *_args, **_kwargs) -> Optional[str]:

    if callable(function_name):
      function_name = f"{function_name.__module__}.{function_name.__name__}"
    else:
      function_name = str(function_name)

    channel = _kwargs.get("channel")
    priority = _kwargs.get("priority")
    job_timeout = _kwargs.get("job_timeout")
    result_ttl = _kwargs.get("result_ttl")
    ttl = _kwargs.get("ttl")
    failure_ttl = _kwargs.get("failure_ttl")
    depends_on = _kwargs.get("depends_on")
    job_id = _kwargs.get("job_id")
    description = _kwargs.get("description")
    on_success = _kwargs.get("on_success")
    on_failure = _kwargs.get("on_failure")
    max_attempts = _kwargs.get("max_attempts")
    args = _kwargs.get("args")
    kwargs = _kwargs.get("kwargs")
    lang = _kwargs.get("lang", self.lang)

    removable_keys = ["lang", "channel", "priority", "job_timeout", "result_ttl", "ttl", "failure_ttl", "depends_on", "job_id", "description", "on_success", "on_failure", "max_attempts", "args", "kwargs"]
    for k in removable_keys:
      if k in _kwargs:
        del _kwargs[k]

    if result_ttl is None:
      result_ttl = self.result_ttl

    if ttl is None:
      ttl = self.ttl

    if failure_ttl is None:
      failure_ttl = self.failure_ttl

    if max_attempts is None:
      max_attempts = self.max_attempts

    if channel is None:
      channel = self.channel

    if priority is None:
      priority = self.priority

    if isinstance(kwargs, dict):
      _kwargs.update(kwargs)

    if isinstance(args, list) or isinstance(args, tuple):
      _args += tuple(args)

    if isinstance(depends_on, list) or isinstance(depends_on, tuple):
      depends_on = list(depends_on)
      for i in range(len(depends_on)):
        depends_on[i] = toOid(depends_on[i])
    elif not depends_on is None:
      depends_on = toOid(depends_on)

    data = {
      "data": {
        "function_name": function_name,
        "job_timeout": job_timeout,
        "description": description,
        "on_success": on_success,
        "on_failure": on_failure,
        "args": _args,
        "kwargs": _kwargs,
      },
      "item_type": "queue",
      "consumer_id": self.consumerId,
      "depends_on": depends_on,
      "result_ttl": result_ttl,
      "ttl": ttl,
      "failure_ttl": failure_ttl,
      "max_attempts": max_attempts,
      "channel": channel,
      "inProgress": False,
      "done": False,
      "attempts": 0,
      "progress": 0,
      "priority": priority,
      "lang": lang,
      "createdAt": datetime.utcnow()
    }

    if not job_id is None:
      data["_id"] = toOid(job_id)

    res: Type[InsertOneResult]  = self.collection.insert_one(data)
    return res.inserted_id  

  def dequeue(self, channel: Optional[str]=None, lang: str = None, job_id: Optional[str]=None) -> Optional[Type[Job]]:
    res = self.collection.find({"done": True}, ["_id"])
    ids = []

    if lang is None:
      lang = self.lang

    for item in list(res):
      oid = item.get("_id")
      if not oid is None:
        ids.append(toOid(oid))

    query = {"lang": lang, "consumer_id": self.consumerId, "item_type": "queue", "inProgress": False, "done": False, "$expr": {"$lt": ["$attempts", "$max_attempts"]}, "$or": [{"depends_on": None}, {"depends_on": {"$not": {"$elemMatch": {"$nin" : ids }}}}]}
    if channel is None:
      channel = self.channel

    query["channel"] = channel
    if not job_id is None:
      query["_id"] = toOid(job_id)

    job = self.collection.find_one_and_update(query, {"$set": {"inProgress": True, "startedAt": datetime.utcnow()}}, sort=[("priority", DESCENDING), ("createdAt", ASCENDING)])

    if not job is None:
      keys = ["depends_on", "result_ttl", "failure_ttl", "ttl", "createdAt", "lang"]
      kwargs = {}
      for k in keys:
        if k in job:
          kwargs[k] = job.get(k)

      return Job(job.get("_id"), job.get("data", {}), self.collection, **kwargs)

    return None