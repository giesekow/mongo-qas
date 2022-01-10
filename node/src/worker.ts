import { Queue } from './queue'
import { Job } from './job'
import { sleep } from './misc'

export interface WorkerOptions {
  channel?: string|string[]
  heartBeat?: number
  verbose?: boolean
  verbosity?: string
  logger?: string|Function
}

export class Worker {
  private queues: Queue[] = []
  private channels: string[] = []
  private working: boolean = false
  private heartBeat: number = 1
  private running: boolean = false
  private job?: Job|undefined
  private verbose: boolean = false
  private logger: any
  private verbosity: string = 'error'

  constructor(queues: Queue|Queue[], options?: WorkerOptions) {
    this.queues = Array.isArray(queues) ? queues : [queues]
    if(options?.heartBeat && options.heartBeat > 1) this.heartBeat = options.heartBeat
    if (options?.channel) {
      this.channels = Array.isArray(options.channel) ? options.channel : [options.channel];
    }
    if (options?.verbose) this.verbose = options.verbose;
    if (options?.verbosity) this.setVerbosity(options.verbosity);
    if (options?.logger) this.setLogger(options.logger)
  }

  setLogger(logger:string|Function) {
    const callback: any = logger;
    if (callback.call) this.logger = callback;
    else if (typeof callback === "string") {
      const cparts: string[] = callback.toString().split(".");
      if (cparts.length > 1) {
        const modName = cparts[0];
        const mod: any = require(modName);
        const func: Function|undefined = cparts.slice(1).reduce((m: any , n: any) => { return m ? m[n] : m }, mod);
        if (func && func.call) {
          this.logger = func
        }
      }
    }
  }

  setVerbosity(verbosity: string) {
    this.verbosity = verbosity;
  }

  async start() {
    this.running = true;
    await this.run();
  }

  stop() {
    this.running = false;
  }

  private async work() {
    this.working = true
    this.job = undefined

    for (let i = 0; i < this.queues.length; i++) {
      const queue = this.queues[i];
      if (this.channels.length === 0) {
        this.job = await queue.dequeue();
        if (this.job) break;
      } else {
        for (let c = 0; c < this.channels.length; c++) {
          const channel = this.channels[c];
          this.job = await queue.dequeue({channel});
          if (this.job) break;
        }
        if (this.job) break;
      }
    }

    if (this.job) {
      await this.runJob(this.job);
      this.working = false;
    } else {
      this.working = false;
    }

  }

  private async runJob(job: Job) {
    const payload = job.payload;
    job.setVerbosity(this.verbosity);
    job.setLogger(this.logger);
    try {
      if (payload) {
        const callback = payload["function_name"];
        if (callback) {
          const cparts: string[] = callback.toString().split(".");
          if (cparts.length > 1) {
            const modName = cparts[0];
            const mod: any = require(modName);
            const func: Function|undefined = cparts.slice(1).reduce((m: any , n: any) => { return m ? m[n] : m }, mod);
            if (func && func.call) {
              const args: any[] = payload.args || [];
              const kwargs: any = payload.kwargs || null;
              if (kwargs) args.push(kwargs);
              const result  = await func.call(func, ...args);
              job.complete(result);
            }
          } else {
            job.error(`Unable to find function ${callback}`)
            if(this.verbose) console.log(`Unable to find function ${callback}`)
          }
        }
      }
    } catch (error) {
      job.error(error.toString())
      if(this.verbose) console.log(error.toString())
    }
  }

  private async run() {
    while (true) {
      if (!this.running) break;
      if (!this.working) await this.work();
      await sleep(this.heartBeat);
    }
  }
}