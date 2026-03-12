import threading
import time
import random
from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:

    # should never hit this, but safety net
    MAX_RETRIES = 1000

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        self._thread = threading.Thread(target=self.__run, daemon=True)
        self._thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if self._thread is not None:
            self._thread.join()


    def __run(self):
        for transaction in self.transactions:
            committed = False
            attempts = 0

            while not committed and attempts < self.MAX_RETRIES:
                attempts += 1
                committed = transaction.run()

                if not committed:
                    time.sleep(random.uniform(0.001, 0.01))

            self.stats.append(committed)
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

