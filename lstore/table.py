import os
from lstore.index import Index
from lstore.page import Page
from time import time
from lstore.page import Page

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
    def __init__(self, name, num_columns, key, bufferpool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.bufferpool = bufferpool
        self.page_directory = {}
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.next_rid = 1
        self.next_page_id = 0 # counter for unique page ids for database
        
        total_columns = 4 + num_columns
        # tracking id of pages instead of the objects themselves
        self.base_page_ids = []
        self.tail_page_ids = []

         

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
                base_page_range = self.base_page_ids[page_range_index]
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
                    value = self.get_latest_column_value(rid, i, indirection, base_page_range, record_index)
                    result.append(value)
                else:
                    result.append(None)
            return result
    
    def get_column_value(self, rid, column_index):
        if rid not in self.page_directory: 
            return None
        loc = self.page_directory[rid]
        range_ids = self.base_page_ids[loc[1]]

        self.bufferpool.pin(range_ids[INDIRECTION_COLUMN])
        indir_page = self.get_or_load_page(range_ids[INDIRECTION_COLUMN])
        offset = loc[2] * 8
        indirection = int.from_bytes(indir_page.data[offset:offset+8], 'little', signed=True)
        self.bufferpool.unpin(range_ids[INDIRECTION_COLUMN])
        
        return self.get_latest_column_value(rid, column_index, indirection, range_ids, loc[2])
    
    def get_latest_column_value(self, base_rid, column_index, indirection, base_page_range_ids, base_record_index):
        
        current_tail_rid = indirection
        while current_tail_rid != 0:
            tail_loc = self.page_directory.get(current_tail_rid)
            if not tail_loc: break
            
            t_range_ids = self.tail_page_ids[tail_loc[1]]
            tail_offset = tail_loc[2] * 8

            self.bufferpool.pin(t_range_ids[SCHEMA_ENCODING_COLUMN])
            schema_page = self.get_or_load_page(t_range_ids[SCHEMA_ENCODING_COLUMN])
            schema_encoding = int.from_bytes(schema_page.data[tail_offset:tail_offset+8], 'little', signed=True)
            self.bufferpool.unpin(t_range_ids[SCHEMA_ENCODING_COLUMN])
            
            if (schema_encoding >> column_index) & 1:
                val_p_id = t_range_ids[4 + column_index]
                self.bufferpool.pin(val_p_id)
                val_page = self.get_or_load_page(val_p_id)
                val = int.from_bytes(val_page.data[tail_offset:tail_offset+8], 'little', signed=True)
                self.bufferpool.unpin(val_p_id)
                return val
            
            ind_p_id = t_range_ids[INDIRECTION_COLUMN]
            self.bufferpool.pin(ind_p_id)
            indir_page = self.get_or_load_page(ind_p_id)
            current_tail_rid = int.from_bytes(indir_page.data[tail_offset:tail_offset+8], 'little', signed=True)
            self.bufferpool.unpin(ind_p_id)

        base_val_p_id = base_page_range_ids[4 + column_index]
        self.bufferpool.pin(base_val_p_id)
        base_val_page = self.get_or_load_page(base_val_p_id)
        val = int.from_bytes(base_val_page.data[base_record_index*8:base_record_index*8+8], 'little', signed=True)
        self.bufferpool.unpin(base_val_p_id)
        return val
    
    def add_base_record(self, columns, schema_encoding):
        rid = self.next_rid
        self.next_rid += 1

        if len(self.base_page_ids) == 0 or not self.get_or_load_page(self.base_page_ids[-1][RID_COLUMN]).has_capacity():
            # create new page range (list of unique IDs for each column)
            total_columns = 4 + self.num_columns
            new_range_ids = []
            for _ in range(total_columns):
                new_id = self.next_page_id
                self.next_page_id += 1
                new_range_ids.append(new_id)
                self.bufferpool.add_page(new_id, Page())
            
            self.base_page_ids.append(new_range_ids)

        current_range_ids = self.base_page_ids[-1]

        column_values = [0, rid, int(time()), int(schema_encoding, 2)] + list(columns)
        
        for i, value in enumerate(column_values):
            page_id = current_range_ids[i]
            page = self.get_or_load_page(page_id)
            self.bufferpool.pin(page_id)
            page.write(value)
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)

        page_range_index = len(self.base_page_ids) - 1

        record_index = self.get_or_load_page(current_range_ids[RID_COLUMN]).num_records - 1
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
        if rid not in self.page_directory:
            return None

        location = self.page_directory[rid]
        base_page_range = self.base_page_ids[location[1]]
        base_offset = location[2] * 8

        # Get latest tail for this base
        current_tail_rid = int.from_bytes(
            base_page_range[INDIRECTION_COLUMN].data[base_offset:base_offset + 8],
            byteorder='little', signed=True
        )

        # Traverse relative_version
        for _ in range(abs(relative_version)):
            if current_tail_rid == 0:
                break
            tail_loc = self.page_directory[current_tail_rid]
            tail_page_range = self.tail_pages[tail_loc[1]]
            tail_offset = tail_loc[2] * 8
            current_tail_rid = int.from_bytes(
                tail_page_range[INDIRECTION_COLUMN].data[tail_offset:tail_offset+8],
                byteorder='little', signed=True
            )

        # Now find latest value for this column starting from current_tail_rid
        cur_rid = current_tail_rid
        while cur_rid != 0:
            tail_loc = self.page_directory[cur_rid]
            t_page_range = self.tail_pages[tail_loc[1]]
            t_offset = tail_loc[2] * 8

            schema_encoding = int.from_bytes(
                t_page_range[SCHEMA_ENCODING_COLUMN].data[t_offset:t_offset+8],
                byteorder='little', signed=True
            )
            schema_bits = format(schema_encoding, f'0{self.num_columns}b')[::-1]

            if schema_bits[column_index] == '1':
                return int.from_bytes(
                    t_page_range[4 + column_index].data[t_offset:t_offset+8],
                    byteorder='little', signed=True
                )

            # Move to previous tail
            cur_rid = int.from_bytes(
                t_page_range[INDIRECTION_COLUMN].data[t_offset:t_offset+8],
                byteorder='little', signed=True
            )

        # Fallback to base
        return int.from_bytes(
            base_page_range[4 + column_index].data[base_offset:base_offset+8],
            byteorder='little', signed=True
        )
        
    import os
from lstore.index import Index
from lstore.page import Page
from time import time
from lstore.page import Page

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
    def __init__(self, name, num_columns, key, bufferpool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.bufferpool = bufferpool
        self.page_directory = {}
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.next_rid = 1
        self.next_page_id = 0 # counter for unique page ids for database
        
        total_columns = 4 + num_columns
        # tracking id of pages instead of the objects themselves
        self.base_page_ids = []
        self.tail_page_ids = []

         

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
                base_page_range = self.base_page_ids[page_range_index]
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
                    value = self.get_latest_column_value(rid, i, indirection, base_page_range, record_index)
                    result.append(value)
                else:
                    result.append(None)
            return result
    
    def get_column_value(self, rid, column_index):
        if rid not in self.page_directory: 
            return None
        loc = self.page_directory[rid]
        range_ids = self.base_page_ids[loc[1]]

        self.bufferpool.pin(range_ids[INDIRECTION_COLUMN])
        indir_page = self.get_or_load_page(range_ids[INDIRECTION_COLUMN])
        offset = loc[2] * 8
        indirection = int.from_bytes(indir_page.data[offset:offset+8], 'little', signed=True)
        self.bufferpool.unpin(range_ids[INDIRECTION_COLUMN])
        
        return self.get_latest_column_value(rid, column_index, indirection, range_ids, loc[2])
    
    def get_latest_column_value(self, base_rid, column_index, indirection, base_page_range_ids, base_record_index):
        
        current_tail_rid = indirection
        while current_tail_rid != 0:
            tail_loc = self.page_directory.get(current_tail_rid)
            if not tail_loc: break
            
            t_range_ids = self.tail_page_ids[tail_loc[1]]
            tail_offset = tail_loc[2] * 8

            self.bufferpool.pin(t_range_ids[SCHEMA_ENCODING_COLUMN])
            schema_page = self.get_or_load_page(t_range_ids[SCHEMA_ENCODING_COLUMN])
            schema_encoding = int.from_bytes(schema_page.data[tail_offset:tail_offset+8], 'little', signed=True)
            self.bufferpool.unpin(t_range_ids[SCHEMA_ENCODING_COLUMN])
            
            if (schema_encoding >> column_index) & 1:
                val_p_id = t_range_ids[4 + column_index]
                self.bufferpool.pin(val_p_id)
                val_page = self.get_or_load_page(val_p_id)
                val = int.from_bytes(val_page.data[tail_offset:tail_offset+8], 'little', signed=True)
                self.bufferpool.unpin(val_p_id)
                return val
            
            ind_p_id = t_range_ids[INDIRECTION_COLUMN]
            self.bufferpool.pin(ind_p_id)
            indir_page = self.get_or_load_page(ind_p_id)
            current_tail_rid = int.from_bytes(indir_page.data[tail_offset:tail_offset+8], 'little', signed=True)
            self.bufferpool.unpin(ind_p_id)

        base_val_p_id = base_page_range_ids[4 + column_index]
        self.bufferpool.pin(base_val_p_id)
        base_val_page = self.get_or_load_page(base_val_p_id)
        val = int.from_bytes(base_val_page.data[base_record_index*8:base_record_index*8+8], 'little', signed=True)
        self.bufferpool.unpin(base_val_p_id)
        return val
    
    def add_base_record(self, columns, schema_encoding):
        rid = self.next_rid
        self.next_rid += 1

        if len(self.base_page_ids) == 0 or not self.get_or_load_page(self.base_page_ids[-1][RID_COLUMN]).has_capacity():
            # create new page range (list of unique IDs for each column)
            total_columns = 4 + self.num_columns
            new_range_ids = []
            for _ in range(total_columns):
                new_id = self.next_page_id
                self.next_page_id += 1
                new_range_ids.append(new_id)
                self.bufferpool.add_page(new_id, Page())
            
            self.base_page_ids.append(new_range_ids)

        current_range_ids = self.base_page_ids[-1]

        column_values = [0, rid, int(time()), int(schema_encoding, 2)] + list(columns)
        
        for i, value in enumerate(column_values):
            page_id = current_range_ids[i]
            page = self.get_or_load_page(page_id)
            self.bufferpool.pin(page_id)
            page.write(value)
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)

        page_range_index = len(self.base_page_ids) - 1

        record_index = self.get_or_load_page(current_range_ids[RID_COLUMN]).num_records - 1
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
        if rid not in self.page_directory:
            return None

        location = self.page_directory[rid]
        base_page_range = self.base_page_ids[location[1]]
        base_offset = location[2] * 8

        # Get latest tail for this base
        current_tail_rid = int.from_bytes(
            base_page_range[INDIRECTION_COLUMN].data[base_offset:base_offset + 8],
            byteorder='little', signed=True
        )

        # Traverse relative_version
        for _ in range(abs(relative_version)):
            if current_tail_rid == 0:
                break
            tail_loc = self.page_directory[current_tail_rid]
            tail_page_range = self.tail_pages[tail_loc[1]]
            tail_offset = tail_loc[2] * 8
            current_tail_rid = int.from_bytes(
                tail_page_range[INDIRECTION_COLUMN].data[tail_offset:tail_offset+8],
                byteorder='little', signed=True
            )

        # Now find latest value for this column starting from current_tail_rid
        cur_rid = current_tail_rid
        while cur_rid != 0:
            tail_loc = self.page_directory[cur_rid]
            t_page_range = self.tail_pages[tail_loc[1]]
            t_offset = tail_loc[2] * 8

            schema_encoding = int.from_bytes(
                t_page_range[SCHEMA_ENCODING_COLUMN].data[t_offset:t_offset+8],
                byteorder='little', signed=True
            )
            schema_bits = format(schema_encoding, f'0{self.num_columns}b')[::-1]

            if schema_bits[column_index] == '1':
                return int.from_bytes(
                    t_page_range[4 + column_index].data[t_offset:t_offset+8],
                    byteorder='little', signed=True
                )

            # Move to previous tail
            cur_rid = int.from_bytes(
                t_page_range[INDIRECTION_COLUMN].data[t_offset:t_offset+8],
                byteorder='little', signed=True
            )

        # Fallback to base
        return int.from_bytes(
            base_page_range[4 + column_index].data[base_offset:base_offset+8],
            byteorder='little', signed=True
        )
        
    def update_record(self, rid, columns):
        if rid not in self.page_directory: 
            return False
        
        location = self.page_directory[rid]
        if location[0] != 'base': 
            return False

        base_range_ids = self.base_page_ids[location[1]]
        base_indirection_page = self.get_or_load_page(base_range_ids[INDIRECTION_COLUMN])
        offset = location[2] * 8
        current_indirection = int.from_bytes(base_indirection_page.data[offset:offset+8], 'little', signed=True)

        if len(self.tail_page_ids) == 0 or not self.get_or_load_page(self.tail_page_ids[-1][RID_COLUMN]).has_capacity():
            total_columns = 4 + self.num_columns
            new_ids = []
            for _ in range(total_columns):
                new_ids.append(self.next_page_id)
                self.bufferpool.add_page(self.next_page_id, Page())
                self.next_page_id += 1
            self.tail_page_ids.append(new_ids)
        
        current_tail_ids = self.tail_page_ids[-1]
        tail_rid = self.next_rid
        self.next_rid += 1

        schema_encoding = int(''.join(['1' if c is not None else '0' for c in columns][::-1]), 2)
        
        meta = [current_indirection, tail_rid, int(time()), schema_encoding]
        for i, val in enumerate(meta + list(columns)):
            
            p_id = current_tail_ids[i]
            p = self.get_or_load_page(p_id)
            self.bufferpool.pin(p_id)
            p.write(val if val is not None else 0) 
            self.bufferpool.mark_dirty(p_id)
            self.bufferpool.unpin(p_id)


        self.bufferpool.pin(base_range_ids[INDIRECTION_COLUMN])
        base_indirection_page.data[offset:offset+8] = tail_rid.to_bytes(8, 'little', signed=True)
        self.bufferpool.mark_dirty(base_range_ids[INDIRECTION_COLUMN])
        self.bufferpool.unpin(base_range_ids[INDIRECTION_COLUMN])

        self.page_directory[tail_rid] = ('tail', len(self.tail_page_ids)-1, self.get_or_load_page(current_tail_ids[0]).num_records - 1)
        return True
    
    def delete_record(self, rid):
        if rid not in self.page_directory:
            return False
        
        del self.page_directory[rid]
        return True
    
    def get_or_load_page(self, page_id):
        page = self.bufferpool.get_page(page_id)
        if page:
            return page
        
        file_path = os.path.join(self.bufferpool.path, f"page_{page_id}.bin")
        new_page = Page()

        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                new_page.data = bytearray(f.read())
                new_page.num_records = len(new_page.data) // 8 

        self.bufferpool.add_page(page_id, new_page)
        return new_page

    def update_record(self, rid, columns):
        if rid not in self.page_directory: 
            return False
        
        location = self.page_directory[rid]
        if location[0] != 'base': 
            return False

        base_range_ids = self.base_page_ids[location[1]]
        
        self.bufferpool.pin(base_range_ids[INDIRECTION_COLUMN])
        base_indirection_page = self.get_or_load_page(base_range_ids[INDIRECTION_COLUMN])
        offset = location[2] * 8
        current_indirection = int.from_bytes(base_indirection_page.data[offset:offset+8], 'little', signed=True)
        

        if len(self.tail_page_ids) == 0:
            must_create_tail = True
        else:
            
            tail_rid_p_id = self.tail_page_ids[-1][RID_COLUMN]
            tail_rid_page = self.get_or_load_page(tail_rid_p_id)
            must_create_tail = not tail_rid_page.has_capacity()

        if must_create_tail:
            total_columns = 4 + self.num_columns
            new_ids = []
            for _ in range(total_columns):
                new_ids.append(self.next_page_id)
                self.bufferpool.add_page(self.next_page_id, Page())
                self.next_page_id += 1
            self.tail_page_ids.append(new_ids)
        
        current_tail_ids = self.tail_page_ids[-1]
        tail_rid = self.next_rid
        self.next_rid += 1

        schema_encoding = 0
        for i, val in enumerate(columns):
            if val is not None:
                schema_encoding |= (1 << i)

        
        meta = [current_indirection, tail_rid, int(time()), schema_encoding]
        
        for i, val in enumerate(meta + list(columns)):
            p_id = current_tail_ids

    
    def delete_record(self, rid):
        if rid not in self.page_directory:
            return False
        
        del self.page_directory[rid]
        return True
    
    def get_or_load_page(self, page_id):
        page = self.bufferpool.get_page(page_id)
        if page:
            return page
        
        file_path = os.path.join(self.bufferpool.path, f"page_{page_id}.bin")
        new_page = Page()

        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                new_page.data = bytearray(f.read())
                new_page.num_records = len(new_page.data) // 8 

        self.bufferpool.add_page(page_id, new_page)
        return new_page

 
