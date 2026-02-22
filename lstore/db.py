import os
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
    def __init__(self, name, num_columns, key, bufferpool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.bufferpool = bufferpool
        self.page_directory = {}
        self.index = Index(self)
        self.merge_threshold_pages = 50 
        self.next_rid = 1
        self.next_page_id = 0

        self.base_page_ids = []
        self.tail_page_ids = []

    def get_or_load_page(self, page_id):
        """Standard recovery logic for Milestone 2 durability."""
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

    def get_record_data(self, rid, projected_columns_index):
        if rid not in self.page_directory:
            return None

        location = self.page_directory[rid]
        record_type, range_idx, rec_idx = location
        if record_type != 'base': return None

        range_ids = self.base_page_ids[range_idx]
        
        
        self.bufferpool.pin(range_ids[INDIRECTION_COLUMN])
        indir_page = self.get_or_load_page(range_ids[INDIRECTION_COLUMN])
        offset = rec_idx * 8
        indirection = int.from_bytes(indir_page.data[offset:offset + 8], 'little', signed=True)
        self.bufferpool.unpin(range_ids[INDIRECTION_COLUMN])

        result = []
        for i in range(self.num_columns):
            if projected_columns_index[i] == 1:
                value = self._get_latest_column_value(rid, i, indirection, range_ids, rec_idx)
                result.append(value)
            else:
                result.append(None)
        return result

    def get_column_value(self, rid, column_index):
        if rid not in self.page_directory: return None
        loc = self.page_directory[rid]
        range_ids = self.base_page_ids[loc[1]]

        self.bufferpool.pin(range_ids[INDIRECTION_COLUMN])
        indir_page = self.get_or_load_page(range_ids[INDIRECTION_COLUMN])
        offset = loc[2] * 8
        indirection = int.from_bytes(indir_page.data[offset:offset+8], 'little', signed=True)
        self.bufferpool.unpin(range_ids[INDIRECTION_COLUMN])
        
        return self._get_latest_column_value(rid, column_index, indirection, range_ids, loc[2])

    def _get_latest_column_value(self, base_rid, column_index, indirection, base_range_ids, base_rec_idx):
        current_tail_rid = indirection
        while current_tail_rid != 0:
            tail_loc = self.page_directory.get(current_tail_rid)
            if not tail_loc: break
            
            t_range_ids = self.tail_page_ids[tail_loc[1]]
            tail_offset = tail_loc[2] * 8

            
            self.bufferpool.pin(t_range_ids[SCHEMA_ENCODING_COLUMN])
            schema_p = self.get_or_load_page(t_range_ids[SCHEMA_ENCODING_COLUMN])
            schema_encoding = int.from_bytes(schema_p.data[tail_offset:tail_offset+8], 'little', signed=True)
            self.bufferpool.unpin(t_range_ids[SCHEMA_ENCODING_COLUMN])

            if (schema_encoding >> column_index) & 1:
                val_id = t_range_ids[4 + column_index]
                self.bufferpool.pin(val_id)
                val_p = self.get_or_load_page(val_id)
                val = int.from_bytes(val_p.data[tail_offset:tail_offset+8], 'little', signed=True)
                self.bufferpool.unpin(val_id)
                return val
            
            
            ind_id = t_range_ids[INDIRECTION_COLUMN]
            self.bufferpool.pin(ind_id)
            ind_p = self.get_or_load_page(ind_id)
            current_tail_rid = int.from_bytes(ind_p.data[tail_offset:tail_offset+8], 'little', signed=True)
            self.bufferpool.unpin(ind_id)

        base_val_id = base_range_ids[4 + column_index]
        self.bufferpool.pin(base_val_id)
        base_p = self.get_or_load_page(base_val_id)
        val = int.from_bytes(base_p.data[base_rec_idx*8 : base_rec_idx*8+8], 'little', signed=True)
        self.bufferpool.unpin(base_val_id)
        return val

    def add_base_record(self, columns, schema_encoding):
        rid = self.next_rid
        self.next_rid += 1

        if not self.base_page_ids or not self.get_or_load_page(self.base_page_ids[-1][RID_COLUMN]).has_capacity():
            new_range = []
            for _ in range(4 + self.num_columns):
                new_id = self.next_page_id
                self.next_page_id += 1
                new_range.append(new_id)
                self.bufferpool.add_page(new_id, Page())
            self.base_page_ids.append(new_range)

        current_range_ids = self.base_page_ids[-1]
        vals = [0, rid, int(time()), int(schema_encoding, 2)] + list(columns)
        
        for i, val in enumerate(vals):
            p_id = current_range_ids[i]
            page = self.get_or_load_page(p_id)
            self.bufferpool.pin(p_id)
            page.write(val)
            self.bufferpool.mark_dirty(p_id)
            self.bufferpool.unpin(p_id)

        rec_idx = self.get_or_load_page(current_range_ids[RID_COLUMN]).num_records - 1
        self.page_directory[rid] = ('base', len(self.base_page_ids) - 1, rec_idx)
        return rid

    def update_record(self, rid, columns):
        if rid not in self.page_directory: return False
        loc = self.page_directory[rid]
        if loc[0] != 'base': return False

        base_range_ids = self.base_page_ids[loc[1]]
        base_offset = loc[2] * 8
        
        self.bufferpool.pin(base_range_ids[INDIRECTION_COLUMN])
        indir_page = self.get_or_load_page(base_range_ids[INDIRECTION_COLUMN])
        curr_indir = int.from_bytes(indir_page.data[base_offset:base_offset+8], 'little', signed=True)
        self.bufferpool.unpin(base_range_ids[INDIRECTION_COLUMN])

        if not self.tail_page_ids or not self.get_or_load_page(self.tail_page_ids[-1][RID_COLUMN]).has_capacity():
            new_range = []
            for _ in range(4 + self.num_columns):
                new_id = self.next_page_id
                self.next_page_id += 1
                new_range.append(new_id)
                self.bufferpool.add_page(new_id, Page())
            self.tail_page_ids.append(new_range)
        
        t_range_ids = self.tail_page_ids[-1]
        t_rid = self.next_rid
        self.next_rid += 1

        schema_val = int(''.join(['1' if c is not None else '0' for c in columns][::-1]), 2)
        meta = [curr_indir, t_rid, int(time()), schema_val]
        
        for i, val in enumerate(meta + list(columns)):
            p_id = t_range_ids[i]
            p = self.get_or_load_page(p_id)
            self.bufferpool.pin(p_id)
            
            write_val = val if val is not None else self._get_latest_column_value(rid, i-4, curr_indir, base_range_ids, loc[2])
            p.write(write_val)
            self.bufferpool.mark_dirty(p_id)
            self.bufferpool.unpin(p_id)

        
        self.bufferpool.pin(base_range_ids[INDIRECTION_COLUMN])
        indir_page.data[base_offset:base_offset+8] = t_rid.to_bytes(8, 'little', signed=True)
        self.bufferpool.mark_dirty(base_range_ids[INDIRECTION_COLUMN])
        self.bufferpool.unpin(base_range_ids[INDIRECTION_COLUMN])

        t_idx = self.get_or_load_page(t_range_ids[RID_COLUMN]).num_records - 1
        self.page_directory[t_rid] = ('tail', len(self.tail_page_ids)-1, t_idx)
        return True

    def delete_record(self, rid):
        if rid not in self.page_directory: return False
        del self.page_directory[rid]
        return True