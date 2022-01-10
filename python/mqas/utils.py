executionCode = """
import sys, json, os, importlib, traceback
from bson import json_util

try:
  payload = json.loads(sys.stdin.read(), object_hook=json_util.object_hook)
  modules = payload.get("modules", [])
  
  for module in modules:
    sys.path.append(os.path.abspath(module))

  temp_stdout = sys.stdout
  temp_stderr = sys.stderr
  stdfile = None

  if "stdout" in payload:
    filename = payload["stdout"]
    if not filename is None:
      stdfile = open(filename, "w")
      sys.stdout = stdfile
      sys.stderr = stdfile

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
        sys.stdout = temp_stdout
        sys.stderr = temp_stderr
        if not stdfile is None:
          stdfile.close()
        data = json.dumps({"result": result}, default=json_util.default)
        sys.stdout.write(data)
      elif callback in globals():
        func = globals()[callback]
        args = payload.get("args", [])
        kwargs = payload.get("kwargs", {})
        result = func(*args, **kwargs)
        sys.stdout = temp_stdout
        sys.stderr = temp_stderr
        if not stdfile is None:
          stdfile.close()
        data = json.dumps({"result": result}, default=json_util.default)
        sys.stdout.write(data)
      else:
        sys.stdout = temp_stdout
        sys.stderr = temp_stderr
        if not stdfile is None:
          stdfile.close()
        err = "Function " + str(callback) + " not found!"
        raise Exception(err)
        #data = json.dumps({"error": {message: err}}, default=json_util.default)
        #sys.stdout.write(data)

except Exception as ex:
  sys.stdout = temp_stdout
  sys.stderr = temp_stderr
  if not stdfile is None:
    stdfile.close()
  errtrace = traceback.format_exc()
  err = "Error " + str(ex)
  data = json.dumps({"error": {"trace": str(errtrace), "message": err}}, default=json_util.default)
  sys.stdout.write(data)

"""