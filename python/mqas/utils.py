executionCode = """
import sys, json, os, importlib, traceback
from bson import json_util

def write_output(data, filename):
  if not filename is None:
    with open(filename, "w") as json_file:
      json.dump(data, json_file, default=json_util.default)

try:
  payload = json.loads(sys.stdin.read(), object_hook=json_util.object_hook)
  modules = payload.get("modules", [])
  
  for module in modules:
    sys.path.append(os.path.abspath(module))

  output_file = payload.get("output")

  if not payload is None:
    callback = payload.get("function_name")
    if not callback is None:
      if str(callback).__contains__("."):
        mod_name, func_name = callback.rsplit(".", 1)
        mod = importlib.import_module(mod_name)
        func = getattr(mod, func_name)
        args = payload.get("args", [])
        kwargs = payload.get("kwargs", {})
        result = func(*args, **kwargs)
        data = {"result": result}
        write_output(data, output_file)
      elif callback in globals():
        func = globals()[callback]
        args = payload.get("args", [])
        kwargs = payload.get("kwargs", {})
        result = func(*args, **kwargs)
        data = {"result": result}
        write_output(data, output_file)
      else:
        err = "Function " + str(callback) + " not found!"
        raise Exception(err)

except Exception as ex:
  errtrace = traceback.format_exc()
  err = "Error " + str(ex)
  data = {"error": {"trace": str(errtrace), "message": err}}
  write_output(data, output_file)

"""