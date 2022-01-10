#!/usr/bin/env node

import { Worker } from '../worker'
import { Queue } from '../queue'
import { ArgumentParser, Action, Namespace } from 'argparse'
import * as YAML from 'yaml';
import { resolve } from 'path'
import { MongoClient } from 'mongodb'


class UpdateAction extends Action {
  call(parser: ArgumentParser, namespace: Namespace, values: string | string[], optionString: string | null): void {
    let initValue = namespace[this.dest] || {}
    initValue = Object.assign({}, initValue, values)
    namespace[this.dest] = initValue;
  }
}

function param_parser(v: any) {
  let d: any = `{value:${v}}`
  const dd = dict_load(d)
  return dd.value;
}

function dict_load(data: any) {
  let d: string = data.toString()
  d = d.trim()
  if (!d.startsWith('{')) {
    d = "{" + d + "}"
  }
  d = d.split(':').map((s: string) => s.trim()).join(': ')
  return YAML.parse(d)
}

function parseArgs(): any {
  const parser: any = new ArgumentParser({description: 'Mongo Queuing and Scheduling Library', add_help: true });
  const subparsers = parser.addSubparsers({dest: 'action'});

  const worker = subparsers.addParser('worker', { description: 'Initialize a worker instance to start working on scheduled jobs' });
  worker.addArgument('channels', { metavar: 'channels', type: String, nargs: '*', default: null, help: 'channels to be monitored by the worker' });
  worker.addArgument(['-u', '--conn'], { dest: 'connection', type: String, default: "mongodb://localhost:27017", help: 'mongodb connection string (default: mongodb://localhost:27017)' });
  worker.addArgument('--dbname', { dest: 'dbName', type: String, default: "jobs", help: 'mongodb database name (default: jobs)' });
  worker.addArgument('--colname', { dest: 'colName', type: String, default: "jobs", help: 'mongodb collection name (default: jobs)' });
  worker.addArgument('--consumer-id', { dest: 'consumerId', type: String, default: null, help: 'worker consumer id (default: null)' });
  worker.addArgument(['-l', '--lang'], { dest: 'lang', type: String, default: "python", help: 'worker language (default: python)' });
  worker.addArgument(['-m', '--modules'], { dest: 'modules', action: "append", type: String, default: [], help: 'additional python module paths' });
  worker.addArgument(['-b', '--heartbeat'], { dest: 'heartBeat', type: 'int', default: 1, help: 'worker heart beat in seconds (default: 1)' });
  worker.addArgument(['-v', '--verbose'], { dest: 'verbose', action: 'storeTrue', type: Boolean, default: false, help: 'worker verbosity' });
  worker.addArgument('--verbosity', { dest: 'verbosity', type: String, default: 'error', help: 'logger verbosity, options are [error, completed, progress] (default: error)' });
  worker.addArgument('--logger', { dest: 'logger', type: String, default: null, help: 'logger callback function (default: null)' });


  const queue = subparsers.addParser('queue', { description: 'Schedule a job into the job queue' });
  queue.addArgument('function_name', {metavar: 'functionName', type: String, help: 'the full name of the entry point function' });
  queue.addArgument('args', { metavar: 'args', type: param_parser, nargs: "*", help: 'positional arguments for entry point function (Arguments are passed as strings for other data types use --kwargs)' });
  queue.addArgument(['-u', '--conn'], { dest: 'connection', type: String, default: "mongodb://localhost:27017", help: 'mongodb connection string (default: mongodb://localhost:27017)' });
  queue.addArgument('--dbname', { dest: 'dbName', type: String, default: "jobs", help: 'mongodb database name (default: jobs)' });
  queue.addArgument('--colname', { dest: 'colName', type: String, default: "jobs", help: 'mongodb collection name (default: jobs)' });
  queue.addArgument('--consumer-id', { dest: 'consumerId', type: String, default: null, help: 'worker consumer id (default: null)' });
  queue.addArgument(['-l', '--lang'], { dest: 'lang', type: String, default: "python", help: 'worker language (default: python)' });
  queue.addArgument(['-c', '--channel'], { dest: 'channel', type: String, default: null, help: 'channel to place the job (default: null)' });
  queue.addArgument(['-p', '--priority'], { dest: 'priority', type: 'int', default: null, help: 'priority of the job (default: null)' });
  queue.addArgument(['-j', '--job-id'], { dest: 'jobId', type: String, default: null, help: 'custom id for this job (default: null)' });
  queue.addArgument(['-k', '--kwargs'], { dest: 'kwargs', action: UpdateAction, type: dict_load, default: {}, help: 'named arguments for the function in yaml or json format' });
  queue.addArgument('--job-timeout', { dest: 'jobTimeout', type: 'int', default: null, help: "job timeout in seconds (default: null)" });
  queue.addArgument('--result-ttl', { dest: 'resultTtl', type: 'int', default: null, help: 'number of seconds to keep the job results (default: null)' });
  queue.addArgument('--ttl', { dest: 'ttl', type: 'int', default: null, help: 'worker heart beat in seconds (default: null)' });
  queue.addArgument('--failure-ttl', { dest: 'failureTtl', type: 'int', default: null, help: "number of seconds to wait upon job failure (default: null)" });
  queue.addArgument('--depends_on', { dest: 'dependsOn', action: "append", type: String, default: null, help: 'job ids of jobs which are to be completed before this job' });
  queue.addArgument('--description', { dest: 'description', type: String, default: null, help: 'description for the job' });
  queue.addArgument('--on-success', { dest: 'onSuccess', type: String, default: null, help: 'function to be invoked on successful execution of the job' });
  queue.addArgument('--on-failure', { dest: 'onFailure', type: String, default: null, help: 'function to be invoked on failure of the job' });
  queue.addArgument('--max-attempts', { dest: 'maxAttempts', type: 'int', default: null, help: 'maximum number of times to attempt executing job in case of failure' });

  const args = parser.parseArgs();
  return args
}

async function runWorker(args: any) {
  const queues: Queue[] = []
  const channels: any = args.channels || null
  const kwargs: any = {dbName: args.dbName, colName: args.colName}
  if (args.lang) kwargs.lang = args.lang
  if (args.consumerId) kwargs.consumerId = args.consumerId

  const client = new MongoClient(args.connection, { useUnifiedTopology: true })
  await client.connect()

  kwargs.connection = client

  if (!channels || channels.length === 0) {
    const queue = new Queue(kwargs);
    queues.push(queue)
  } else {
    for (let i = 0; i < channels.length; i++) {
      kwargs.channel = channels[i];
      const queue = new Queue(kwargs)
      queues.push(queue);
    }
  }

  if (queues.length > 0) {
    let modules: any[] = args.modules || []
    for (let i = 0; i < modules.length; i++) {
      const fullpath = resolve(args.modules[i])
      process.env.NODE_PATH = process.env.NODE_PATH ? `${process.env.NODE_PATH}:${fullpath}` : fullpath
    }
    process.env.NODE_PATH = process.env.NODE_PATH ? `${process.env.NODE_PATH}:${process.cwd()}` : process.cwd()
    require('module').Module._initPaths();
    const worker = new Worker(queues, {heartBeat: args.heartBeat, verbose: args.verbose, verbosity: args.verbosity, logger: args.logger})
    if (!channels || channels.length === 0) {
      console.log(`**Started worker with heart beat of ${args.heartBeat} second(s)`)
    } else {
      console.log(`**Started worker to monitor channels ${channels}, with heart beat of ${args.heartBeat} second(s)`)
    }
    worker.start()
  } else {
    console.log("No channels were specified");
  }
}

async function runQueue(args: any): Promise<any> {
  const kwargs: any = {dbName: args.dbName, connection: args.connection, colName: args.colName}
  if (args.lang) kwargs.lang = args.lang
  if (args.consumerId) kwargs.consumerId = args.consumerId
  if (!args.function_name) throw new Error('function name not provided!')

  const queue = new Queue(kwargs);

  const keys: string[] = ['lang', 'channel', 'priority', 'jobTimeout', 'resultTtl', 'ttl', 'failureTtl', 'dependsOn', 'jobId', 'description', 'onSuccess', 'onFailure', 'maxAttempts', 'args', 'kwargs']
  const params: any = {}

  for(const k of keys){
    if (args[k]) params[k] = args[k]
  }

  const jobId = await queue.enqueue(args.function_name, params)
  console.log(`Job Successfully queued, ${jobId}`)
  return jobId
}

function main() {
  const args = parseArgs()
  const action = args.action;
  switch (action) {
    case 'worker':
      runWorker(args)
      break;
    case 'queue':
      runQueue(args)
      .then(() => {
        process.exit();
      })
      .catch((err: Error) => {
        console.log(err.message)
        process.exit();
      })
    default:
      break;
  }
}

main()