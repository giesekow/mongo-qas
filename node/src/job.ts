import { Collection } from 'mongodb';
import { toOid } from './misc'

export class Job {
  id: any
  collection?: Collection
  payload: any
  kwargs: any
  result: any
  logger: any
  verbosity: string = 'error'

  constructor(id: any, data: any, collection: Collection, kwargs?: any) {
    this.id = toOid(id);
    this.collection = collection;
    this.kwargs = kwargs;
    this.payload = data;
    this.result = null;
    if (kwargs.logger) this.setLogger(kwargs.logger)
    if (kwargs.verbosity) this.setVerbosity(kwargs.verbosity)
  }

  data (): any {
    return this.payload;
  }

  setResult(result: any): void {
    this.result = result;
  }

  async complete(result?: any): Promise<boolean> {
    let res = false;
    if (this.collection) {
      await this.collection.updateOne({_id: this.id}, {$set: {progress: 100, inProgress: true, done: true, result: result, completedAt: new Date(Date.now())}});
      res = true;
    }
    
    if (!this.payload) return res;
    try {
      const callback = this.payload["on_success"];
      if (callback) {
        const cparts: string[] = callback.toString().split(".")
        if (cparts.length > 1) {
          const modName = cparts[0];
          const mod: any = require(modName);
          const func: Function|undefined = cparts.slice(1).reduce((m: any , n: any) => { return m ? m[n] : m }, mod);
          if (func && func.call) {
            const args: any[] = this.payload.args || [];
            const kwargs: any = this.payload.kwargs || null;
            if (kwargs) args.push(kwargs)
            await func.call(func, ...args)
          }
        }
      }
    } catch (error) {
      this.log("error", undefined, {errorMessage: error.toString()})
    }

    this.log("completed", undefined, {result})
    return res;
  }

  async error(msg: any): Promise<boolean> {
    let res = false;
    if (this.collection) {
      await this.collection.updateOne({_id: this.id}, {$inc: {attempts: 1}, $set: {inProgress: false, error: true, lastErrorAt: new Date(Date.now()), errorMessage: msg}})
      res = true;
    }

    if (!this.payload) return res;
    try {
      const callback = this.payload["on_failure"];
      if (callback) {
        const cparts: string[] = callback.toString().split(".")
        if (cparts.length > 1) {
          const modName = cparts[0];
          const mod: any = require(modName);
          const func: Function|undefined = cparts.slice(1).reduce((m: any , n: any) => { return m ? m[n] : m }, mod);
          if (func && func.call) {
            const args: any[] = this.payload.args || [];
            const kwargs: any = this.payload.kwargs || null;
            if (kwargs) args.push(kwargs)
            await func.call(func, ...args)
          }
        }
      }
    } catch (error) {
      this.log("error", undefined, {errorMessage: error.toString()})
    }

    this.log("error", undefined, {errorMessage: msg})
    return res;
  }

  log(type: string, args?: any[], kwargs?: any) {
    if (this.logger && this.logger.call) {
      if (this.verbosity.toLowerCase().indexOf(type) >= 0) {
        const payload = this.payload
        let thisArgs: any[] = payload.args || [];
        const thisKwargs: any = payload.kwargs || null;
        let k: any = null;
        if (args) thisArgs = thisArgs.concat(args)
        if (thisKwargs || kwargs) {
          k = Object.assign({}, thisKwargs || {}, kwargs || {})
        }
        if (k) thisArgs.push(k)
        const result  = this.logger.call(this.logger, ...thisArgs)
      }
    }
  }

  async progress(percent: number, msg?: any): Promise<boolean> {
    let res = false;
    if (this.collection) {
      await this.collection.updateOne({_id: this.id}, {$set: {progress: percent, progressMessage: msg, lastProgressAt: new Date(Date.now())}});
      res = true;
    }
    
    this.log("progress", undefined, {percent, progressMessage: msg})
    return res;
  }

  setLogger(logger: any) {
    this.logger = logger
  }

  setVerbosity(verbosity: string) {
    this.verbosity = verbosity
  }

  async release(): Promise<boolean> {
    if (this.collection) {
      await this.collection.updateOne({_id: this.id}, {$set: {inProgress: false, error: false, done: false, releasedAt: new Date(Date.now()), attempts: 0}});
      return true;
    }
    return false;
  }
}