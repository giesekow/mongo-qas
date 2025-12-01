import re
from datetime import timedelta
from bson.objectid import ObjectId
import platform,socket,re,uuid,psutil,logging,hashlib


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

def getSystemInfo():
  try:
    info={}
    info['platform']=platform.system()
    info['platform-release']=platform.release()
    info['platform-version']=platform.version()
    info['architecture']=platform.machine()
    info['hostname']=socket.gethostname()
    info['ip-address']=socket.gethostbyname(socket.gethostname())
    info['mac-address']=':'.join(re.findall('..', '%012x' % uuid.getnode()))
    info['processor']=platform.processor()
    info['ram']=str(round(psutil.virtual_memory().total / (1024.0 **3)))+" GB"
    return info
  except Exception as e:
    logging.exception(e)
    return {}
  
def get_device_uid():
    data = []

    # MAC address (hardware-based)
    mac = uuid.getnode()
    data.append(str(mac))

    # Hostname
    data.append(socket.gethostname())

    # OS information
    data.append(platform.system())
    data.append(platform.node())
    data.append(platform.release())
    data.append(platform.version())
    data.append(platform.machine())
    data.append(platform.processor())

    raw = "|".join(data)

    # Hash into fixed-length ID
    return hashlib.sha256(raw.encode()).hexdigest()
