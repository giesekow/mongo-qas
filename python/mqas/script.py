from .worker import Worker
from .queue import Queue
import yaml
from argparse import Action

def dict_load(data):
  data = str(data).strip()
  if not data.startswith("{"):
    data = "{" + data + "}"

  data = ": ".join([s.strip() for s in str(data).split(":")])
  return yaml.load(data, Loader=yaml.FullLoader)

class UpdateAction(Action):
  def __init__(self, option_strings, dest, nargs=None, **kwargs):
    if nargs is not None:
      raise ValueError("nargs not allowed")
    super().__init__(option_strings, dest, **kwargs)

  def __call__(self, parser, namespace, values, option_string=None):
    init_value = getattr(namespace, self.dest, {})
    init_value.update(values)
    setattr(namespace, self.dest, init_value)

def param_parser(v):
  d = "{" + f"value: {v}" + "}"
  d = dict_load(d)
  return d.get("value")

def parse_args():
  import argparse
  parser = argparse.ArgumentParser(description='Mongo Queuing and Scheduling Library', add_help=True)
  subparsers = parser.add_subparsers(dest='action')

  worker = subparsers.add_parser('worker', description='Initialize a worker instance to start working on scheduled jobs')

  worker.add_argument('channels', metavar='channels', type=str, nargs='*', default=None, help='channels to be monitored by the worker')
  worker.add_argument('-u', '--conn', dest='db_conn', type=str, default="mongodb://localhost:27017",  help='mongodb connection string (default: mongodb://localhost:27017)')
  worker.add_argument('--dbname', dest='db_name', type=str, default="jobs",  help='mongodb database name (default: jobs)')
  worker.add_argument('--colname', dest='col_name', type=str, default="jobs",  help='mongodb collection name (default: jobs)')
  worker.add_argument('--consumer-id', dest='consumerId', type=str, default=None,  help='worker consumer id (default: None)')
  worker.add_argument('-l', '--lang', dest='lang', type=str, default="python",  help='worker language (default: python)')
  worker.add_argument('-m', '--modules', dest='modules', action="append", type=str, default=[], help='additional python module paths')
  worker.add_argument('-b', '--heartbeat', dest='heartbeat', type=int, default=1,  help='worker heart beat in seconds (default: 1)')
  worker.add_argument('--logger', dest='logger', type=str, default=None,  help='logger callback function (default: None)')
  worker.add_argument('--log-file', dest='log_file', type=str, default=None,  help='path to log file (default: None)')
  worker.add_argument('--verbosity', dest='verbosity', type=str, default="error",  help='logger verbosity, options are [error, completed, progress] (default: error)')
  worker.add_argument('-e', '--executable', dest='executables', action=UpdateAction, type=dict_load, default={},  help='python executable paths for running job based on channels.')

  queue = subparsers.add_parser('queue', description='Schedule a job into the job queue')
  queue.add_argument('function_name', metavar='function_name', type=str, help='the full name of the entry point function')
  queue.add_argument('args', metavar='args', type=param_parser, nargs="*", help='positional arguments for entry point function (Arguments are passed as strings for other data types use --kwargs)')
  queue.add_argument('-u', '--conn', dest='db_conn', type=str, default="mongodb://localhost:27017",  help='mongodb connection string (default: mongodb://localhost:27017)')
  queue.add_argument('--dbname', dest='db_name', type=str, default="jobs",  help='mongodb database name (default: jobs)')
  queue.add_argument('--colname', dest='col_name', type=str, default="jobs",  help='mongodb collection name (default: jobs)')
  queue.add_argument('--consumer-id', dest='consumerId', type=str, default=None,  help='worker consumer id (default: None)')
  queue.add_argument('-l', '--lang', dest='lang', type=str, default="python",  help='worker language (default: python)')
  queue.add_argument('-c', '--channel', dest='channel', type=str, default=None, help='channel to place the job (default: None)')
  queue.add_argument('-p', '--priority', dest='priority', type=int, default=None,  help='priority of the job (default: None)')
  queue.add_argument('-j', '--job-id', dest='job_id', type=str, default=None, help='custom id for this job (default: None)')
  queue.add_argument('-k', '--kwargs', dest='kwargs', action=UpdateAction, type=dict_load, default={}, help='named arguments for the function in yaml or json format')
  queue.add_argument('--job-timeout', dest='job_timeout', type=int, default=None,  help=f"job timeout in seconds (default: None)")
  queue.add_argument('--result-ttl', dest='result_ttl', type=int, default=None,  help='number of seconds to keep the job results (default: None)')
  queue.add_argument('--ttl', dest='ttl', type=int, default=None,  help='worker heart beat in seconds (default: None)')
  queue.add_argument('--failure-ttl', dest='failure_ttl', type=int, default=None,  help=f"number of seconds to wait upon job failure (default: None)")
  queue.add_argument('--depends_on', dest='depends_on', action="append", type=str, default=None,  help='job ids of jobs which are to be completed before this job')
  queue.add_argument('--description', dest='description', type=str, default=None,  help='description for the job')
  queue.add_argument('--on-success', dest='on_success', type=str, default=None,  help='function to be invoked on successful execution of the job')
  queue.add_argument('--on-failure', dest='on_failure', type=str, default=None,  help='function to be invoked on failure of the job')
  queue.add_argument('--max-attempts', dest='max_attempts', type=int, default=None,  help='maximum number of times to attempt executing job in case of failure')

  initialize = subparsers.add_parser('init', description='Prepares database for job scheduling and worker')
  initialize.add_argument('-u', '--conn', dest='db_conn', type=str, default="mongodb://localhost:27017",  help='mongodb connection string (default: mongodb://localhost:27017)')
  initialize.add_argument('--dbname', dest='db_name', type=str, default="jobs",  help='mongodb database name (default: jobs)')
  initialize.add_argument('--colname', dest='col_name', type=str, default="jobs",  help='mongodb collection name (default: jobs)')


  args = parser.parse_args()

  return args

def main():
  args = parse_args()
  action = args.action
  
  if str(action).lower() == "worker":
    run_worker(args)
  elif str(action).lower() == "queue":
    run_queue(args)
  elif str(action).lower() == "init":
    run_init(args)

def run_worker(args):
  from pymongo import MongoClient
  import os, sys
  client = MongoClient(args.db_conn)
  queues = ()
  channels = args.channels
  kwargs = dict(db_name=args.db_name, col_name=args.col_name)

  if not args.consumerId is None:
    kwargs['consumerId'] = args.consumerId

  if not args.lang is None:
    kwargs["lang"] = args.lang

  if (channels is None) or len(channels) == 0:
    queue = Queue(client, **kwargs)
    queues += (queue,)
  else:
    for channel in channels:
      kwargs['channel'] = channel
      queue = Queue(client, **kwargs)
      queues += (queue,)
  
  if len(queues) > 0:
    sys.path.append(os.getcwd())
    for module in args.modules:
      sys.path.append(os.path.abspath(module))

    modulePaths = [os.path.abspath(module) for module in args.modules]

    worker = Worker(queues, heart_beat=args.heartbeat, logger=args.logger, verbosity=args.verbosity, executables=args.executables, logFile=args.log_file, modulePaths=modulePaths)
    if (channels is None) or len(channels) == 0:
      print(f"**Started worker with heart beat of {args.heartbeat} second(s)", flush=True)
    else:
      print(f"**Started worker to monitor channels", channels, f"with heart beat of {args.heartbeat} second(s)", flush=True)

    worker.start()
  else:
    print("No channels were specified")

def run_queue(args):
  from pymongo import MongoClient
  import os, sys
  client = MongoClient(args.db_conn)

  kwargs = dict(db_name=args.db_name, col_name=args.col_name)
  if not args.consumerId is None:
    kwargs['consumerId'] = args.consumerId
    
  if not args.lang is None:
    kwargs["lang"] = args.lang

  queue = Queue(client, **kwargs)

  keys = ['lang', 'channel', 'priority', 'job_timeout', 'result_ttl', 'ttl', 'failure_ttl', 'depends_on', 'job_id', 'description', 'on_success', 'on_failure', 'max_attempts', 'args', 'kwargs']
  params = {}
  for k in keys:
    if hasattr(args, k):
      params[k] = getattr(args, k)

  jobId = queue.enqueue(args.function_name, **params)
  print(f"Job Successfully queued, {jobId}")
  return jobId

def run_init(args):
  from pymongo import MongoClient
  client = MongoClient(args.db_conn)
  db = client.get_database(args.db_name)
  coll = db.get_collection(args.col_name)
  coll.create_index("expireAt", expireAfterSeconds=0)
  