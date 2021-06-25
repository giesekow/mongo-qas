'use strict';
import { MongoClient, Db, Collection } from 'mongodb';
import { toOid, parseTimeToSeconds, sleep } from './misc';
import { Job } from './job';

export interface QueueOptions {
  connection?: MongoClient|Db|Collection|string,
  consumerId?: string,
  channel?: string,
  priority?: number,
  jobTimeout?: string|number,
  resultTtl?: string|number,
  ttl?: string|number,
  failureTtl?: string|number,
  maxAttempts?: number,
  dbName?: string,
  colName?: string,
  lang?: string,
}

export interface EnqueueOptions {
  [index: string]: any,
  lang?: string,
  channel?: string,
  priority?: number,
  jobTimeout?: string | number,
  resultTtl?: string | number,
  ttl?: string | number,
  failureTtl?: string | number,
  dependsOn?: any | any[]
  jobId?: any
  description?: string
  onSuccess?: string|Function
  onFailure?: string|Function
  maxAttempts?: number
  args?: any[]
  kwargs?: {[index: string]: any}
}

export interface DequeueOptions {
  channel?: string, 
  jobId?: any,
  lang?: string,
}

export class Queue {
  consumerId: string = 'default-customer-id';
  channel: string = 'default';
  priority: number = 0;
  jobTimeout: number = 60 * 60;
  resultTtl: number = 500;
  ttl: any = null;
  failureTtl: number = 60*60*24*365
  maxAttempts: number = 1
  dbName: string = 'jobs'
  colName: string = 'jobs'
  collection?: Collection
  awaiting: boolean = false
  lang: string = 'node'

  constructor(options?: QueueOptions) {
    const opts: QueueOptions = options || {};
    this.channel = opts.channel || this.channel;
    this.consumerId = opts.consumerId || this.consumerId;
    this.colName = opts.colName || this.colName;
    this.dbName = opts.dbName || this.dbName;
    this.failureTtl =  parseTimeToSeconds(opts.failureTtl, this.failureTtl);
    this.ttl =  parseTimeToSeconds(opts.ttl, this.ttl);
    this.resultTtl =  parseTimeToSeconds(opts.resultTtl, this.resultTtl);
    this.jobTimeout = parseTimeToSeconds(opts.jobTimeout, this.jobTimeout);
    this.priority = opts.priority || this.priority;
    this.maxAttempts = opts.maxAttempts || this.maxAttempts;
    this.lang = opts.lang || this.lang;
    let url = 'mongodb://localhost:27017';
    let colResolved = false;
    if (opts.connection) {
      const conn: any = opts.connection;
      if (opts.connection instanceof MongoClient) {
        this.collection = opts.connection.db(this.dbName).collection(this.colName);
        colResolved = true;
      } else if (opts.connection instanceof Db) {
        this.collection = opts.connection.collection(this.colName);
        colResolved = true;
      } else if (typeof opts.connection !== 'string') {
        if (('find' in opts.connection) && ('findOneAndUpdate' in opts.connection) && ('updateOne' in opts.connection) && ('insertOne' in opts.connection)) {
          this.collection = opts.connection;
          colResolved = true;
        }
      } else if (typeof opts.connection === 'string') {
        url = opts.connection;
      }
    }
    if (!colResolved) {
      const client = new MongoClient(url, { useUnifiedTopology: true });
      this.awaiting = true;
      client.connect()
      .then((c: MongoClient) => {
        this.collection = c.db(this.dbName).collection(this.colName);
        this.awaiting = false;
      })
      .catch(() => {
        this.awaiting = false;
      })
    }
  }

  async checkConnection(): Promise<boolean> {
    while(this.awaiting) {
      await sleep(0.01);
    }
    if (this.collection) return true
    return false;
  }

  async enqueue(functionName: string|Function, options?: EnqueueOptions): Promise<any> {

    const hasConnection = await this.checkConnection()
    if (!hasConnection) throw new Error('MongoDB connection not properly configured!')
    if (!functionName) throw new Error('function name not provided!');

    let funcName = typeof functionName == 'string' ? functionName : functionName.name;
    let onSuccess = null;
    let onFailure = null;
    let args: any[] = options?.args || [];
    let kwargs: any = options?.kwargs || null;
    let dependsOn: any = null;
    let jobId: any = options?.jobId
    const lang = options?.lang || this.lang

    const rkeys = ['lang', 'channel', 'priority', 'jobTimeout', 'resultTtl', 'ttl', 'failureTtl', 'dependsOn', 'jobId', 'description', 'onSuccess', 'onFailure', 'maxAttempts', 'args', 'kwargs']

    if (options?.onSuccess) {
      onSuccess = typeof options.onSuccess == 'string' ? options.onSuccess : options.onSuccess.name;
    }

    if (options?.onFailure) {
      onFailure = typeof options.onFailure == 'string' ? options.onFailure : options.onFailure.name;
    }

    const data: any = {
      'data': {
        'function_name': funcName,
        'job_timeout': options?.jobTimeout ? parseTimeToSeconds(options.jobTimeout) : this.jobTimeout,
        'description': options?.description || null,
        'on_success': onSuccess,
        'on_failure': onFailure,
        'args': args
      },
      'item_type': 'queue',
      'consumer_id': this.consumerId,
      'depends_on': dependsOn,
      'lang': lang,
      'result_ttl': options?.resultTtl ? parseTimeToSeconds(options.resultTtl, this.resultTtl) : this.resultTtl,
      'ttl': options?.ttl ? parseTimeToSeconds(options.ttl, this.ttl) : this.ttl,
      'failure_ttl': options?.failureTtl ? parseTimeToSeconds(options.failureTtl, this.failureTtl) : this.failureTtl,
      'max_attempts': options?.maxAttempts || this.maxAttempts,
      'channel': options?.channel || this.channel,
      'inProgress': false,
      'done': false,
      'attempts': 0,
      'progress': 0,
      'priority': options?.priority || this.priority,
      'createdAt': new Date(Date.now())
    }

    if(jobId) data['_id'] = toOid(jobId)

    const opts: any = options || {};
    for (const k of rkeys) delete opts[k];

    if (Object.keys(opts).length > 0) kwargs = Object.assign({}, opts, kwargs);

    data.data.kwargs = kwargs

    if (this.collection) {
      const job = await this.collection.insertOne(data);
      return job.insertedId
    }

    throw new Error('MongoDB connection not properly configured!')
  }

  async dequeue(options?: DequeueOptions): Promise<Job|undefined> {
    const hasConnection = await this.checkConnection()
    if (!hasConnection) throw new Error('MongoDB connection not properly configured!')

    if (this.collection) {
      const doneJobs = this.collection.find({done: true}, {projection: {_id: true}})
      const ids: any[] = []
      let hasNext: boolean = await doneJobs.hasNext()
      while(hasNext) {
        const item: any = await doneJobs.next();
        ids.push(toOid(item._id))
        hasNext = await doneJobs.hasNext()
      }

      const query: any = {'lang': options?.lang || this.lang, 'consumer_id': this.consumerId, 'item_type': 'queue', 'inProgress': false, 'done': false, '$expr': {'$lt': ['$attempts', '$max_attempts']}, '$or': [{'depends_on': null}, {'depends_on': {'$not': {'$elemMatch': {'$nin' : ids }}}}]}
      query['channel'] = options?.channel || this.channel
      
      if (options?.jobId){
        query['_id'] = toOid(options.jobId)
      }

      const job = await this.collection.findOneAndUpdate(query, {'$set': {'inProgress': true, 'startedAt': new Date(Date.now())}}, {sort: {'priority': -1, 'createdAt': 1}})

      if (job.value) {
        const keys = ['lang', 'depends_on', 'result_ttl', 'failure_ttl', 'ttl', 'createdAt']
        const kwargs: any = {}
        for(const k of keys) {
          if(k in job)
            kwargs[k] = job.value[k]
        }
        return new Job(job.value._id, job.value.data, this.collection, kwargs)
      }
    } else {
      throw new Error('MongoDB connection not properly configured!')
    }
  }

};