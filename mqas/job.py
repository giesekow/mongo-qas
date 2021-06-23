from pymongo.collection import Collection
from typing import Type
from datetime import datetime
from .misc import toOid

class Job:
  def __init__(self, id, data, collection: Type[Collection], **kwargs):
    self.id = toOid(id)
    self.coll = collection
    self.payload = data
    self.kwargs = kwargs
    self.result = None

  def data(self):
    return self.payload

  def set_result(self, result):
    self.result = result
    self.coll.update_one({"_id": self.id}, {"$set": {"result": self.result}})
    return True

  def complete(self, result=None):
    if result is None:
      result = self.result
    
    self.coll.update_one({"_id": self.id}, {"$set": {"progress": 100, "inProgress": False, "done": True, "result": result, "completedAt": datetime.utcnow()}})
    return True

  def error(self, msg=None):   
    self.coll.update_one({"_id": self.id}, {"$inc": {"attempts": 1}, "$set": {"inProgress": False, "error": True, "lastErrorAt": datetime.utcnow(), "errorMessage": msg}})
    return True

  def progress(self, percent):
    self.coll.update_one({"_id": self.id}, {"$set": {"progress": percent, "lastProgressAt": datetime.utcnow()}})
    return True

  def release(self):
    self.coll.update_one({"_id": self.id}, {"$set": {"inProgress": False, "error": False, "done": False, "releasedAt": datetime.utcnow(), "attempts": 0}})
    return True