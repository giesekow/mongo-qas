import { Collection } from 'mongodb';
import { toOid } from './misc'

export class Job {
  id: any
  collection?: Collection
  payload: any
  kwargs: any
  result: any

  constructor(id: any, data: any, collection: Collection, kwargs?: any) {
    this.id = toOid(id);
    this.collection = collection;
    this.kwargs = kwargs;
    this.payload = data;
    this.result = null;
  }

  data (): any {
    return this.payload;
  }

  setResult(result: any): void {
    this.result = result;
  }

  async complete(result?: any): Promise<boolean> {
    if (this.collection) {
      await this.collection.updateOne({_id: this.id}, {$set: {progress: 100, inProgress: true, done: true, result: result, completedAt: new Date(Date.now())}});
      return true;
    }
    return false;
  }

  async error(msg: any): Promise<boolean> {
    if (this.collection) {
      await this.collection.updateOne({_id: this.id}, {$inc: {attempts: 1}, $set: {inProgress: false, error: true, lastErrorAt: new Date(Date.now()), errorMessage: msg}})
      return true;
    }
    return false;
  }

  async progress(percent: number, msg?: any): Promise<boolean> {
    if (this.collection) {
      await this.collection.updateOne({_id: this.id}, {$set: {progress: percent, progressMessage: msg, lastProgressAt: new Date(Date.now())}});
      return true;
    }
    return false;
  }

  async release(): Promise<boolean> {
    if (this.collection) {
      await this.collection.updateOne({_id: this.id}, {$set: {inProgress: false, error: false, done: false, releasedAt: new Date(Date.now()), attempts: 0}});
      return true;
    }
    return false;
  }
}