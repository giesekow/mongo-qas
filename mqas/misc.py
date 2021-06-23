import re
from datetime import timedelta
from bson.objectid import ObjectId


regex = re.compile(r'((?P<weeks>\d+?)w)?((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')


def parse_time(time_str):
    parts = regex.match(time_str)
    if not parts:
      return
    parts = parts.groupdict()
    time_params = {}
    for name, param in parts.items():
      if param:
        time_params[name] = int(param)

    return timedelta(**time_params)

def parse_time_to_seconds(time_str):
    if isinstance(time_str, int) or isinstance(time_str, float):
      return int(time_str)
      
    parts = regex.match(time_str)
    if not parts:
      return
    parts = parts.groupdict()
    time_params = {}
    for name, param in parts.items():
      if param:
        time_params[name] = int(param)

    return timedelta(**time_params).total_seconds()

def toOid(id):
  oid = id
  try:
    oid = ObjectId(oid)
  except:
    pass
  
  return oid