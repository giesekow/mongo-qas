from .worker import Worker
from .queue import Queue
from ruamel.yaml import YAML
from argparse import Action

def dict_load(data):
  data = str(data).strip()
  if not data.startswith("{"):
    data = "{" + data + "}"

  data = ": ".join([s.strip() for s in str(data).split(":")])
  yaml = YAML(typ="safe", pure=True)
  yaml.default_flow_style = False
  return yaml.load(data)

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

  parser.add_argument('-u', '--conn', dest='db_conn', type=str, default="mongodb://localhost:27017",  help='mongodb connection string (default: mongodb://localhost:27017)')
  parser.add_argument('--dbname', dest='db_name', type=str, default="jobs",  help='mongodb database name (default: jobs)')
  parser.add_argument('--colname', dest='col_name', type=str, default="jobs",  help='mongodb collection name (default: jobs)')
  parser.add_argument('--consumer-id', dest='consumerId', type=str, default=None,  help='worker consumer id (default: None)')

  worker = subparsers.add_parser('worker', description='Initialize a worker instance to start working on scheduled jobs')
  worker.add_argument('channels', metavar='channels', type=str, nargs='*', default=None, help='channels to be monitored by the worker')
  worker.add_argument('-m', '--modules', dest='modules', action="append", type=str, default=[], help='additional python module paths')
  worker.add_argument('-b', '--heartbeat', dest='heartbeat', type=int, default=1,  help='worker heart beat in seconds (default: 1)')

  queue = subparsers.add_parser('queue', description='Schedule a job into the job queue')
  queue.add_argument('function_name', metavar='function_name', type=str, help='the full name of the entry point function')
  queue.add_argument('args', metavar='args', type=param_parser, nargs="*", help='positional arguments for entry point function (Arguments are passed as strings for other data types use --kwargs)')
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

  args = parser.parse_args()

  return args

def main():
  args = parse_args()
  action = args.action
  
  if str(action).lower() == "worker":
    run_worker(args)
  elif str(action).lower() == "queue":
    run_queue(args)

def run_worker(args):
  from pymongo import MongoClient
  import os, sys
  client = MongoClient(args.db_conn)
  queues = ()
  channels = args.channels
  kwargs = dict(db_name=args.db_name, col_name=args.col_name)

  if not args.consumerId is None:
    kwargs['consumerId'] = args.consumerId

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

    worker = Worker(queues, heart_beat=args.heartbeat)
    if (channels is None) or len(channels) == 0:
      print(f"**Started worker with heart beat of {args.heartbeat} second(s)")
    else:
      print(f"**Started worker to monitor channels", channels, f"with heart beat of {args.heartbeat} second(s)")

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

  queue = Queue(client, **kwargs)

  keys = ['channel', 'priority', 'job_timeout', 'result_ttl', 'ttl', 'failure_ttl', 'depends_on', 'job_id', 'description', 'on_success', 'on_failure', 'max_attempts', 'args', 'kwargs']
  params = {}
  for k in keys:
    if hasattr(args, k):
      params[k] = getattr(args, k)

  jobId = queue.enqueue(args.function_name, **params)
  print(f"Job Successfully queued, {jobId}")
  return jobId