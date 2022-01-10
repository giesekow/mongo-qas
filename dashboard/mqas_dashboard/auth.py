from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
import os, json

auth = HTTPBasicAuth()

def load_users():
  fn = os.environ.get("AUTH_FILE")
  users = {}
  if not fn is None:
    if fn.endswith(".json"):
      with open(fn, "r") as json_file:
        data = json.load(json_file)
        if isinstance(data, dict):
          for k in data:
            users[k] = generate_password_hash(data[k])
  return users

@auth.verify_password
def verify_password(username, password):
  users = load_users()
  if username in users and check_password_hash(users.get(username), password):
    return username