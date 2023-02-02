from pymongo import DESCENDING, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo import MongoClient
from bson import ObjectId
from typing import Union, Optional, Type
from datetime import datetime, timedelta
from flask import g
import os

class DB:
  def __init__(self, connection: Union[str, Type[Collection], Type[Database], Type[MongoClient]] = None, db_name: Optional[str]="jobs", col_name: Optional[str]="jobs") -> None:
    
    if connection is None:
      connection = "mongodb://localhost:27017"

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

  def get_workers(self):
    items = self.collection.find({"is_worker": True}).sort("createdAt", DESCENDING)
    return list(items)

  def get_worker(self, worker_id):
    item = self.collection.find_one({"is_worker": True, "_id": ObjectId(worker_id)})
    return item

  def get_jobs(self, limit=0, skip=0, query={}):
    query["is_worker"] = {"$ne": True}
    
    if limit == "all":
      limit = 0
    else:
      limit = int(limit)

    count = self.collection.count_documents(query)
    items = self.collection.find(query).sort("createdAt", DESCENDING).sort("updatedAt", DESCENDING).skip(skip).limit(limit)
    return list(items), count

  def get_job(self, job_id):
    item = self.collection.find_one({"is_worker": {"$ne": True}, "_id": ObjectId(job_id)})
    return item

  def requeue_job(self, job_id):
    job = self.get_job(job_id)
    expireAt = None
    ttl = job.get("ttl")
    if not ttl is None:
      if int(ttl) > 0:
        expireAt = datetime.utcnow() + timedelta(seconds=int(ttl))

    res = self.collection.update_one({"is_worker": {"$ne": True}, "_id": ObjectId(job_id)}, {"$set": {"done": False, "error": False, "inProgress": False, "errorMessage": None, "result": None, "expireAt": expireAt, "attempts": 0}})
    return res

  def remove_job(self, job_id):
    res = self.collection.delete_one({"is_worker": {"$ne": True}, "_id": ObjectId(job_id)})
    return res

def get_variables():
  connection = os.environ.get("DB_CONNECTION", "mongodb://localhost:27017")
  db_name = os.environ.get("DB_NAME", "jobs")
  col_name = os.environ.get("DB_COL_NAME", "jobs")
  return dict(connection=connection, db_name=db_name, col_name=col_name)

def get_db():
  if 'db' not in g:
    kwargs = get_variables()
    g.db = DB(**kwargs)

  return g.db