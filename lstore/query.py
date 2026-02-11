from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids or len(rids) == 0:
                return False
                
            rid = rids[0]
            self.table.delete_record(rid)
            self.table.index.remove_key(self.table.key, primary_key)
            return True
        except Exception:
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        
        try:
            if len(columns) != self.table.num_columns:
                return False

            existing_rids = self.table.index.locate(self.table.key, columns[self.table.key])
            if existing_rids and len(existing_rids) > 0:
                return False
            
            rid = self.table.add_base_record(columns, schema_encoding)
            self.table.index.insert_key(columns[self.table.key], rid)
            return True
        except Exception:
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            rids = self.table.index.locate(search_key_index, search_key)
            results = []
            for rid in rids:
                data = self.table.get_record_data(rid, projected_columns_index)
                if data is not None:
                    actual_pk = self.table.get_column_value(rid, self.table.key)
                    results.append(Record(rid, actual_pk, data))
            return results
        except Exception:
            return False
            

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            rids = self.table.index.locate(search_key_index, search_key)
            results = []
            for rid in rids:
                data = self.table.get_version_data(rid, projected_columns_index, relative_version)
                if data:
                    actual_pk = self.table.get_column_value(rid, self.table.key)
                    results.append(Record(rid, actual_pk, data))
            return results
        except Exception:
            return False

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids or len(rids) == 0:
                return False
            
            rid = rids[0]
            return self.table.update_record(rid, *columns)
        except Exception:
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            total = 0
            found = False
            for key in range(start_range, end_range + 1):
                rids = self.table.index.locate(self.table.key, key)
                if rids:
                    rid = rids[0]
                    val = self.table.get_column_value(rid, aggregate_column_index)
                    total += val
                    found = True
            return total if found else False
        except Exception:
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            total = 0
            found = False
            for key in range(start_range, end_range + 1):
                rids = self.table.index.locate(self.table.key, key)
                if rids:
                    rid = rids[0]
                    val = self.table.get_version_column_value(rid, aggregate_column_index, relative_version)
                    total += val
                    found = True
            return total if found else False
        except Exception:
            return False

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False