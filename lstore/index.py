"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

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
      return self.indices[column][value]
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
      search_results = []
      if self.indices[column] is None:
          return search_results
      for value in self.indices[column]:
          if begin <= value <= end:
              search_results.extend(self.indices[column][value])
              
      return search_results
    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = {}
    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
      self.indices[column_number] = None
