from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = {}  # Dictionary to hold table name (key) to Table object (Value) mapping
        self.bufferpool = BufferPool(buffer_size = 100)

    # Not required for milestone1
    def open(self, path):
        self.bufferpool.open(path)
        print(f"Database opened at {path}")

    def close(self):
        self.bufferpool.close()
        print("Database closed, all dirty pages flushed")

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # Check if table with the given name already exists
        if name in self.tables:
            return self.tables[name]
        
        # Else, create a new table
        table = Table(name, num_columns, key_index, self.bufferpool)
        self.tables[name] = table
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables.get(name, None)
