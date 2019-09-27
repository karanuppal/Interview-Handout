import mock_db
import uuid
from worker import worker_main
from threading import Thread
import time
import logging

logger = logging.getLogger(__name__)

def release_lock(db):
    db.delete_one({'is_worker_running': True})

# fixed for one process run
lock_id = uuid.uuid4()
def acquire_lock(db):
    """
    Insertion into the db is not an atomic operation.
    To ensure that the lock gets acquired only once
    the key is set to a fixed value and the thread that writes first
    will get the lock and other writes will fail.
    """
    obj = {'_id': lock_id, 'is_worker_running': True}
    db.insert_one(obj)

def lock_is_free(db):
    """
        CHANGE ME, POSSIBLY MY ARGS

        Return whether the lock is free
    """
    # check if lock is free

    # acquire the lock
    try:
        acquire_lock(db)
        return True
    except:
        return False


def attempt_run_worker(worker_hash, give_up_after, db, retry_interval):
    """
        CHANGE MY IMPLEMENTATION, BUT NOT FUNCTION SIGNATURE

        Run the worker from worker.py by calling worker_main

        Args:
            worker_hash: a random string we will use as an id for the running worker
            give_up_after: if the worker has not run after this many seconds, give up
            db: an instance of MockDB
            retry_interval: continually poll the locking system after this many seconds
                            until the lock is free, unless we have been trying for more
                            than give_up_after seconds
    """

    start = time.time()
    while True:
        # Check if lock is free
        # If it's free, acquire it before moving forward
        if lock_is_free(db):
            try:
                worker_main(worker_hash, db)
                release_lock(db)
                break
            except:
                # release lock on crash
                release_lock(db)
                logger.warning("Worker id: {} crashed.".format(worker_hash))
        else:
            time.sleep(retry_interval)
            # check time elapsed from start
            # every time it wakes up
            stop = time.time()
            # give up if time elapsed greater than give_up_after
            if (stop - start) >= give_up_after:
                break



if __name__ == "__main__":
    """
        DO NOT MODIFY

        Main function that runs the worker five times, each on a new thread
        We have provided hard-coded values for how often the worker should retry
        grabbing lock and when it should give up. Use these as you see fit, but
        you should not need to change them
    """

    db = mock_db.DB()
    threads = []
    for _ in range(25):
        t = Thread(target=attempt_run_worker, args=(uuid.uuid1(), 2000, db, 0.1))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
