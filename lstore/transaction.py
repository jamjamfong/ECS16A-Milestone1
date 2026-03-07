from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = [] # Queued operations
        self._undo_log = [] # Data that needs to roll back
        self._locked_records = set() # Locks

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    # Execute queued queries in sequence. Before writing a query, it prepares rollback info. If query returns false, it will call abort(). If successful it calls commit()
    def run(self):
        self._undo_log = []
        for query, table, args in self.queries:
            query_name = getattr(query, "__name__", "")
            undo_entry = None

            if query_name == "insert":
                undo_entry = ("insert", table, args[table.key])
            elif query_name in ("update", "delete"):
                primary_key = args[0]
                rids = table.index.locate(table.key, primary_key)
                if rids:
                    rid = rids[0]
                    before_image = table.get_record_data(rid, [1] * table.num_columns)
                    if before_image is not None:
                        undo_entry = (query_name, table, before_image)

            result = query(*args)

            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            if undo_entry is not None:
                self._undo_log.append(undo_entry)
        return self.commit()

    # Replays undo log in reverse to abort commits
    def abort(self):
        for op_type, table, payload in reversed(self._undo_log):
            if op_type == "insert":
                key = payload
                rids = table.index.locate(table.key, key)
                if rids:
                    table.delete_record(rids[0])
            elif op_type == "update":
                previous_values = payload
                key = previous_values[table.key]
                rids = table.index.locate(table.key, key)
                if rids:
                    restored_columns = [None] * table.num_columns
                    for i in range(table.num_columns):
                        if i != table.key:
                            restored_columns[i] = previous_values[i]
                    table.update_record(rids[0], restored_columns)
            elif op_type == "delete":
                previous_values = payload
                rid = table.add_base_record(previous_values, '0' * table.num_columns)
                for col_idx in range(table.num_columns):
                    if table.index.indices[col_idx] is not None:
                        table.index.add_to_index(col_idx, previous_values[col_idx], rid)

        self._undo_log = []
        self._locked_records.clear()
        return False

    # Clears temporary state
    def commit(self):
        self._undo_log = []
        self._locked_records.clear()
        return True
