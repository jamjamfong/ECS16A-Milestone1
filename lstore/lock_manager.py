from lstore.index import Index
import threading

class RWLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.reader = 0
        self.writer = False 
        
    def acquire_rlock(self):         
        with self.lock:
            if self.writer: return False
            self.reader += 1
            return True

    def release_rlock(self):
        with self.lock:
            self.reader -= 1

    def acquire_wlock(self):
        with self.lock:
            if self.reader or self.writer: return False
            self.writer = True
            return True

    def release_wlock(self):
        with self.lock:
            self.writer = False

    def upgrade_to_wlock(self):
        with self.lock:
            if self.reader == 1 and not self.writer:
                self.reader -= 1
                self.writer = True
                return True
            return False


class LockManager:
    def __init__(self):
        self.locks = {}
        self.map_lock = threading.Lock()

    def get_lock(self, record_id):
        with self.map_lock:
            if record_id not in self.locks:
                self.locks[record_id] = RWLock()
            return self.locks[record_id]

    def acquire_shared(self, transaction, record_id):
        if transaction.held_locks.get(record_id) in ('X', 'S'): return True
        if self.get_lock(record_id).acquire_rlock():
            transaction.held_locks[record_id] = 'S'
            return True
        return False

    def acquire_exclusive(self, transaction, record_id):
        held = transaction.held_locks.get(record_id)
        if held == 'X': return True
        lock = self.get_lock(record_id)
        if held == 'S' and not lock.upgrade_to_wlock(): return False
        if held != 'S' and not lock.acquire_wlock(): return False
        transaction.held_locks[record_id] = 'X'
        return True

    def release_all(self, transaction):
        for record_id, lock_type in list(transaction.held_locks.items()):
            lock = self.get_lock(record_id)
            lock.release_rlock() if lock_type == 'S' else lock.release_wlock()
        transaction.held_locks.clear()