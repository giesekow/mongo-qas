import threading, psutil, datetime, time
from pymongo.collection import Collection
from pymongo import MongoClient
from bson.objectid import ObjectId as _ObjectId

__STORAGE = threading.local()

def make_oid(oid=None):
    """A function to convert string to mongodb ObjectId or generate a new ObjectId

    Parameters
    ----------
    oid : str, Optional
      A string to be converted to ObjectId

    Returns
    -------
    objectId: ObjectId
      The ObjectId representation of oid or new ObjectId

    """
    try:
        if oid is None:
            return _ObjectId()

        if isinstance(oid, (list, tuple)):
            return list((_ObjectId(oi) for oi in oid))
        
        return _ObjectId(oid)
    except Exception:
        return None

def set_value(key, value):
    setattr(__STORAGE, key, value)

def get_value(key, default=None):
    if hasattr(__STORAGE, key):
        return getattr(__STORAGE, key)
    else:
        return default

def get_job_id():
    job_id = get_value("__job_id")
    return job_id

def get_worker_id():
    worker_id = get_value("__worker_id")
    return worker_id

def get_jobs_db():
    DB_CLIENT = get_value("__db_client")
    DB_OBJ = get_value("__db_obj")

    if DB_CLIENT is None:
        db_name = get_value("__db_name")
        db_conn = get_value("__db_conn")

        if db_conn is None or db_name is None:
            return None
        
        DB_CLIENT = MongoClient(db_conn)
        DB_OBJ = DB_CLIENT[db_name]

    return DB_OBJ

def get_jobs_coll() -> Collection:
    db = get_jobs_db()
    db_coll = get_value("__db_coll")

    if db is None or db_coll is None:
        return None
    
    return db[db_coll]


class MemoryLocker:
    def __init__(self, memory_mb=None, timeout=60, update_interval=10):
        self.memory = memory_mb
        self.reservation_id = None
        self.__timeout = timeout
        self.__update_interval = min(5, update_interval, timeout)
        self.__updater_thread = None
        self.__stop_event = threading.Event()
        self.coll = get_jobs_coll()

    def lock(self, memory_mb):
        self.memory = memory_mb
        return self.__wait()

    def release(self):
        self.__stop()

    def __request_scheduling(self, worker_id):
        expire_at = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=5)
        result = self.coll.find_one_and_update(
            {
                "is_reservation_lock": True,
                "is_locked": False
            },
            {
                "$setOnInsert": {
                    "is_reservation_lock": True,
                    "is_locked": True,
                    "worker_id": worker_id,
                    "expireAt": expire_at
                }
            },
            upsert=True,
            return_document=True
        )

        if result:
            return result.get("_id")

        return None

    def __reserve_memory(self):
        worker_id = get_worker_id()

        lock_id = self.__request_scheduling(worker_id)

        if not lock_id:
            return False
        
        avail_mem = psutil.virtual_memory().available / (1024**2) # MB
        
        job_id = get_job_id()
        # Get reserved memory
        coll = self.coll
        
        workers = coll.find({ "is_worker": True }, ["_id"])
        worker_ids = list(w.get("_id") for w in list(workers) if str(w.get("_id")) != worker_id)

        query = {
            "is_reservation": True,
            "worker_id": { "$in": worker_ids }
        }

        res = coll.find(query, ["ram"])
        total_res = sum(r.get("ram", 0) for r in res)
        can_reserve = (avail_mem - total_res) >= self.memory

        if can_reserve:
            res = coll.find_one({"worker_id": make_oid(worker_id), "is_reservation": True}, ["_id"])
            if res:
                self.reservation_id = res.get("_id")
                coll.find_one_and_update({"_id": make_oid(self.reservation_id)}, {"$set": {"ram": self.memory, "job_id": make_oid(job_id)}}, projection={"_id": True})
            else:
                data = {
                    "is_reservation": True,
                    "worker_id": make_oid(worker_id),
                    "job_id": make_oid(job_id),
                    "ram": self.memory,
                    "expireAt": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=self.__timeout)
                }
                res = coll.insert_one(data)
                self.reservation_id = res.inserted_id
            
            coll.delete_one({"_id": lock_id})

            return True
        
        coll.delete_one({"_id": lock_id})

        return False

    def __start_updater(self):
        """Background thread that refreshes the reservation periodically."""
        def updater():
            coll = self.coll
            while not self.__stop_event.is_set() and self.reservation_id is not None:
                try:
                    coll.find_one_and_update(
                        {"_id": make_oid(self.reservation_id)},
                        {"$set": {"expireAt": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=self.__timeout)}}
                    )
                except Exception as e:
                    print("MemoryLocker updater error:", e, flush=True)

                # Wait for update interval or stop event
                self.__stop_event.wait(self.__update_interval)

        self.__updater_thread = threading.Thread(target=updater, daemon=True)
        self.__updater_thread.start()

    def __wait(self):
        if not self.coll:
            return
        
        while not self.__reserve_memory():
            time.sleep(1)

        self.__start_updater()

    def __enter__(self):
        if not self.memory:
            return self
        
        self.__wait()
        
        return self
    
    def __stop(self):
        # Stop the updater thread
        self.__stop_event.set()
        if self.__updater_thread:
            self.__updater_thread.join(timeout=1)

        # Release reservation
        if self.reservation_id:
            coll = self.coll
            coll.delete_one({"_id": make_oid(self.reservation_id)})
            self.reservation_id = None
        
    def __exit__(self, exc_type, exc_value, traceback):
        return self.__stop()