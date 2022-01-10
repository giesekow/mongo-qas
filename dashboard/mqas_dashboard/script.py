from .app import create_app

def parse_args():
  import argparse
  parser = argparse.ArgumentParser(description='Dashboard for the Mongo Queuing and Scheduling Library', add_help=True)
  parser.add_argument('-u', '--conn', dest='db_conn', type=str, default="mongodb://localhost:27017",  help='mongodb connection string (default: mongodb://localhost:27017)')
  parser.add_argument('--dbname', dest='db_name', type=str, default="jobs",  help='mongodb database name (default: jobs)')
  parser.add_argument('--colname', dest='col_name', type=str, default="jobs",  help='mongodb collection name (default: jobs)')
  parser.add_argument('--auth', dest='auth_file', type=str, default=None,  help='path to a json file which contains list of user accounts for basic authentication')
  parser.add_argument('--debug', dest='debug_mode', action='store_true', help='run flask in debug mode')

  args = parser.parse_args()

  return args

def main():
  import os
  args = parse_args()

  env = {"DB_CONNECTION": args.db_conn, "DB_NAME": args.db_name, "DB_COL_NAME": args.col_name, "AUTH_FILE": args.auth_file, "FLASK_APP": "mqas_dashboard"}
  for k in env:
    if not env[k] is None:
      os.environ[k] = env[k]

  app = create_app()
  app.run(debug=args.debug_mode, use_reloader=False, host="0.0.0.0")
