from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.next_rid = 1
        
        total_columns = 4 + num_columns
        self.base_pages = [[Page() for _ in range(total_columns)]]
        self.tail_pages = [[Page() for _ in range(total_columns)]]

    def __merge(self):
        print("merge is happening")
        pass
    
    def get_record_data(self, rid, projected_columns_index):
            if rid not in self.page_directory:
                return None

            result = []
            location = self.page_directory[rid]
            record_type, page_range_index, record_index = location
            if record_type == 'base':
                base_page_range = self.base_pages[page_range_index]
            else:
                return None
            offset = record_index * 8
            indirection = int.from_bytes(
                base_page_range[INDIRECTION_COLUMN].data[offset:offset + 8],
                byteorder='little',
                signed=True
            )
        
            for i in range(self.num_columns):
                if projected_columns_index[i] == 1:
                    value = self._get_latest_column_value(rid, i, indirection, base_page_range, record_index)
                    result.append(value)
                else:
                    result.append(None)
            return result
    
    def get_column_value(self, rid, column_index):
        if isinstance(rid, list):
            if len(rid) == 0:
                return None
            rid = rid[0]
        
        if rid not in self.page_directory:
            return None
        
        # Get base record location
        location = self.page_directory[rid]
        record_type, page_range_index, record_index = location
        
        if record_type == 'base':
            base_page_range = self.base_pages[page_range_index]
        else:
            return None
        
        offset = record_index * 8
        
        # Get indirection
        indirection = int.from_bytes(
            base_page_range[INDIRECTION_COLUMN].data[offset:offset + 8],
            byteorder='little',
            signed=True
        )
        
        return self._get_latest_column_value(rid, column_index, indirection, base_page_range, record_index)
    
    def _get_latest_column_value(self, base_rid, column_index, indirection, base_page_range, base_record_index):
        # If no tail records, read from base
        if indirection == 0:
            offset = base_record_index * 8
            return int.from_bytes(
                base_page_range[4 + column_index].data[offset:offset + 8],
                byteorder='little',
                signed=True
            )
        
        # Follow tail chain to find latest update for this column
        current_tail_rid = indirection
        
        while current_tail_rid != 0:
            if current_tail_rid not in self.page_directory:
                break
            
            tail_location = self.page_directory[current_tail_rid]
            _, tail_page_range_index, tail_record_index = tail_location
            tail_page_range = self.tail_pages[tail_page_range_index]
            
            tail_offset = tail_record_index * 8
            
            # Check schema encoding to see if this column was updated
            schema_encoding = int.from_bytes(
                tail_page_range[SCHEMA_ENCODING_COLUMN].data[tail_offset:tail_offset + 8],
                byteorder='little',
                signed=True
            )
            
            # Convert to binary string and check column bit
            schema_bits = format(schema_encoding, f'0{self.num_columns}b')
            if schema_bits[column_index] == '1':
                # This tail record has the update
                return int.from_bytes(
                    tail_page_range[4 + column_index].data[tail_offset:tail_offset + 8],
                    byteorder='little',
                    signed=True
                )
            
            # Move to previous tail record
            current_tail_rid = int.from_bytes(
                tail_page_range[INDIRECTION_COLUMN].data[tail_offset:tail_offset + 8],
                byteorder='little',
                signed=True
            )
        
        # No update found in tail, read from base
        offset = base_record_index * 8
        return int.from_bytes(
            base_page_range[4 + column_index].data[offset:offset + 8],
            byteorder='little',
            signed=True
        )
    
    def add_base_record(self, columns, schema_encoding):
        rid = self.next_rid
        self.next_rid += 1
        current_page_range = self.base_pages[-1]
        
        if not current_page_range[0].has_capacity():
            total_columns = 4 + self.num_columns
            self.base_pages.append([Page() for _ in range(total_columns)])
            current_page_range = self.base_pages[-1]
        
        current_page_range[INDIRECTION_COLUMN].write(0)  # No tail record initially
        current_page_range[RID_COLUMN].write(rid)
        current_page_range[TIMESTAMP_COLUMN].write(int(time()))
        current_page_range[SCHEMA_ENCODING_COLUMN].write(int(schema_encoding, 2))
        
        for i, value in enumerate(columns):
            current_page_range[4 + i].write(value)
        
        page_range_index = len(self.base_pages) - 1
        record_index = current_page_range[0].num_records - 1
        self.page_directory[rid] = ('base', page_range_index, record_index)
        
        return rid
    
    def get_version_data(self, rid, projected_columns_index, relative_version):
        
        if rid not in self.page_directory:
            return None
        
        result = []
        
        for i in range(self.num_columns):
            if projected_columns_index[i] == 1:
                value = self.get_version_column_value(rid, i, relative_version)
                result.append(value)
            else:
                result.append(None)
        
        return result
    
    def get_version_column_value(self, rid, column_index, relative_version):
        if isinstance(rid, list):
            if len(rid) == 0:
                return None
            rid = rid[0]
        
        if rid not in self.page_directory:
            return None
        
        location = self.page_directory[rid]
        record_type, page_range_index, record_index = location
        
        if record_type == 'base':
            base_page_range = self.base_pages[page_range_index]
        else:
            return None
        
        offset = record_index * 8
        
        indirection = int.from_bytes(
            base_page_range[INDIRECTION_COLUMN].data[offset:offset + 8],
            byteorder='little',
            signed=True
        )
        
        # Version 0 means latest
        if relative_version == 0:
            return self._get_latest_column_value(rid, column_index, indirection, base_page_range, record_index)
        
        # Build version chain (newest to oldest)
        current_tail_rid = indirection
        version_chain = []
        while current_tail_rid != 0:
            if current_tail_rid not in self.page_directory:
                break
            version_chain.append(current_tail_rid)
            
            tail_location = self.page_directory[current_tail_rid]
            _, tail_page_range_index, tail_record_index = tail_location
            tail_page_range = self.tail_pages[tail_page_range_index]
            
            tail_offset = tail_record_index * 8
            current_tail_rid = int.from_bytes(
                tail_page_range[INDIRECTION_COLUMN].data[tail_offset:tail_offset + 8],
                byteorder='little',
                signed=True
            )
        
        # For version -1: skip 1 tail record (go to version before latest)
        # For version -2: skip 2 tail records
        target_version_index = abs(relative_version)
        
        # If we don't have enough versions, return base
        if target_version_index >= len(version_chain):
            base_offset = record_index * 8
            return int.from_bytes(
                base_page_range[4 + column_index].data[base_offset:base_offset + 8],
                byteorder='little',
                signed=True
            )
        
        # Get the tail record at the target version
        tail_rid = version_chain[target_version_index]
        tail_location = self.page_directory[tail_rid]
        _, tail_page_range_index, tail_record_index = tail_location
        tail_page_range = self.tail_pages[tail_page_range_index]
        tail_offset = tail_record_index * 8
        
        # Read the column value from this tail record
        return int.from_bytes(
            tail_page_range[4 + column_index].data[tail_offset:tail_offset + 8],
            byteorder='little',
            signed=True
        )
        
    def update_record(self, rid, columns):
        if rid not in self.page_directory:
            return False
        
        location = self.page_directory[rid]
        record_type, page_range_index, record_index = location
        
        if record_type == 'base':
            page_range = self.base_pages[page_range_index]
        else:
            page_range = self.tail_pages[page_range_index]
        
        offset = record_index * 8
        current_indirection = int.from_bytes(
            page_range[INDIRECTION_COLUMN].data[offset:offset + 8],
            byteorder='little',
            signed=True
        )
        
        tail_rid = self.next_rid
        self.next_rid += 1
        
        current_tail_range = self.tail_pages[-1]
        if not current_tail_range[0].has_capacity():
            total_columns = 4 + self.num_columns
            self.tail_pages.append([Page() for _ in range(total_columns)])
            current_tail_range = self.tail_pages[-1]
        
        schema_encoding = ['0'] * self.num_columns
        for i, value in enumerate(columns):
            if value is not None:
                schema_encoding[i] = '1'
        schema_encoding_str = ''.join(schema_encoding)
        
        current_tail_range[INDIRECTION_COLUMN].write(current_indirection)  # Point to previous version
        current_tail_range[RID_COLUMN].write(rid)  # Original base RID
        current_tail_range[TIMESTAMP_COLUMN].write(int(time()))
        current_tail_range[SCHEMA_ENCODING_COLUMN].write(int(schema_encoding_str, 2))
        
        base_location = self.page_directory[rid]
        _, base_page_range_index, base_record_index = base_location
        base_page_range = self.base_pages[base_page_range_index]

        for i, value in enumerate(columns):
            if value is not None:
                current_tail_range[4 + i].write(value)
            else:
                prev_val = self._get_latest_column_value(rid, i, current_indirection, base_page_range, base_record_index)
                current_tail_range[4 + i].write(prev_val)
        
        page_range[INDIRECTION_COLUMN].data[offset:offset + 8] = tail_rid.to_bytes(8, byteorder='little', signed=True)
        
        tail_page_range_index = len(self.tail_pages) - 1
        tail_record_index = current_tail_range[0].num_records - 1
        self.page_directory[tail_rid] = ('tail', tail_page_range_index, tail_record_index)
        
        return True
    
    def delete_record(self, rid):
        if rid not in self.page_directory:
            return False
        
        del self.page_directory[rid]
        return True
 
