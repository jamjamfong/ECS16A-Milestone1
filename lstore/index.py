"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree

class Index:

    def __init__(self, table):
        self.table = table
        self.indices = [None] *  table.num_columns
        self.create_index(self.table.key)
    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):
      if self.indices[column] is None or value not in self.indices[column]:
          return []
      return self.indices[column].get(value, [])
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
      search_results = []
      if self.indices[column] is None:
          return search_results
      for rid_list in self.indices[column].values(begin, end):
          search_results.extend(rid_list)
      return search_results
    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = OOBTree()
        for rid, location in self.table.page_directory.items():
            value = self.table.get_latest_value(rid, column_number)
            self.add_to_index(column_number, value, rid)

    def add_to_index(self, column, value, rid):
        if self.indices[column] is None:
            return
        if value not in self.indices[column]:
            self.indices[column][value] = []
        if rid not in self.indices[column][value]:
            self.indices[column][value].append(rid)

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        if column_number != self.table.key:
            self.indices[column_number] = None
      
    def insert_key(self, value, rid):
        column = self.table.key
        if self.indices[column] is None:
            self.create_index(column)
        if value not in self.indices[column]:
            self.indices[column][value] = []
        self.indices[column][value].append(rid)
        
    def remove_key(self, column, value):
        if rid in self.indices[column][value]:
            self.indices[column][value].remove(rid)
