from lstore.index import Index
from lstore.page import Page
from time import time
import os

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
        self.num_columns = num_columns
        self.key = key
        self.page_directory = {}
        self.index = Index(self)
        self.merge_threshold_pages = 50
        self.next_rid = 1
        self.bufferpool = bufferpool
        total_columns = 4 + num_columns
        self.base_pages = [[self.bufferpool.add_page(i, Page()) for i in range(total_columns)]]
        self.tail_pages = [[self.bufferpool.add_page(i + total_columns, Page()) for i in range(total_columns)]]

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

    def read_int(self, page_id, offset):
        page = self.get_or_load_page(page_id)
        return int.from_bytes(page.data[offset:offset + 8], byteorder='little', signed=True)

    def write_int(self, page_id, value, flush=True):
        page = self.get_or_load_page(page_id)
        page.write(value)
        self.bufferpool.mark_dirty(page_id)
        if flush:
            self.bufferpool.flush_to_disk(page_id, page)

    def _get_latest_column_value(self, base_rid, column_index, indirection, base_page_range, base_record_index):
        current_tail_rid = indirection
        while current_tail_rid != 0:
            tail_location = self.page_directory.get(current_tail_rid)
            if not tail_location:
                break
            _, tail_page_range_index, tail_record_index = tail_location
            tail_page_range = self.tail_pages[tail_page_range_index]
            tail_offset = tail_record_index * 8
            schema_encoding = self.read_int(tail_page_range[SCHEMA_ENCODING_COLUMN], tail_offset)
            schema_bits = format(schema_encoding, f'0{self.num_columns}b')[::-1]
            if schema_bits[column_index] == '1':
                return self.read_int(tail_page_range[4 + column_index], tail_offset)
            current_tail_rid = self.read_int(tail_page_range[INDIRECTION_COLUMN], tail_offset)
        offset = base_record_index * 8
        return self.read_int(base_page_range[4 + column_index], offset)

    def get_record_data(self, rid, projected_columns_index):
        if rid not in self.page_directory:
            return None
        result = []
        record_type, page_range_index, record_index = self.page_directory[rid]
        if record_type != 'base':
            return None
        base_page_range = self.base_pages[page_range_index]
        offset = record_index * 8
        indirection = self.read_int(base_page_range[INDIRECTION_COLUMN], offset)
        for i in range(self.num_columns):
            if projected_columns_index[i] == 1:
                value = self._get_latest_column_value(rid, i, indirection, base_page_range, record_index)
                result.append(value)
            else:
                result.append(None)
        return result

    def get_column_value(self, rid, column_index):
        if isinstance(rid, list) and len(rid) > 0:
            rid = rid[0]
        if rid not in self.page_directory:
            return None
        record_type, page_range_index, record_index = self.page_directory[rid]
        if record_type != 'base':
            return None
        base_page_range = self.base_pages[page_range_index]
        offset = record_index * 8
        indirection = self.read_int(base_page_range[INDIRECTION_COLUMN], offset)
        return self._get_latest_column_value(rid, column_index, indirection, base_page_range, record_index)

    def add_base_record(self, columns, schema_encoding):
        rid = self.next_rid
        self.next_rid += 1
        current_page_range = self.base_pages[-1]
        if not self.get_or_load_page(current_page_range[0]).has_capacity():
            total_columns = 4 + self.num_columns
            new_range = [self.bufferpool.add_page(i + len(self.base_pages)*total_columns, Page()) for i in range(total_columns)]
            self.base_pages.append(new_range)
            current_page_range = self.base_pages[-1]
        self.write_int(current_page_range[INDIRECTION_COLUMN], 0)
        self.write_int(current_page_range[RID_COLUMN], rid)
        self.write_int(current_page_range[TIMESTAMP_COLUMN], int(time()))
        self.write_int(current_page_range[SCHEMA_ENCODING_COLUMN], int(schema_encoding, 2))
        for i, value in enumerate(columns):
            self.write_int(current_page_range[4 + i], value)
        page_range_index = len(self.base_pages) - 1
        record_index = self.get_or_load_page(current_page_range[0]).num_records - 1
        self.page_directory[rid] = ('base', page_range_index, record_index)
        for pid in current_page_range:
            self.bufferpool.flush_to_disk(pid, self.get_or_load_page(pid))
        return rid

    def update_record(self, rid, columns):
        if rid not in self.page_directory:
            return False
        if len(columns) != self.num_columns:
            return False
        if columns[self.key] is not None:
            return False
        record_type, base_page_range_index, base_record_index = self.page_directory[rid]
        if record_type != 'base':
            return False
        base_page_range = self.base_pages[base_page_range_index]
        base_offset = base_record_index * 8
        current_indirection = self.read_int(base_page_range[INDIRECTION_COLUMN], base_offset)
        columns = list(columns)
        schema_bits = ['0'] * self.num_columns
        for i, val in enumerate(columns):
            if val is not None:
                schema_bits[i] = '1'
        schema_encoding_val = int(''.join(schema_bits[::-1]), 2)
        tail_rid = self.next_rid
        self.next_rid += 1
        if not self.get_or_load_page(self.tail_pages[-1][0]).has_capacity():
            total_columns = 4 + self.num_columns
            new_range = [self.bufferpool.add_page(i + len(self.base_pages + self.tail_pages)*total_columns, Page()) for i in range(total_columns)]
            self.tail_pages.append(new_range)
        current_tail_range = self.tail_pages[-1]
        self.write_int(current_tail_range[INDIRECTION_COLUMN], current_indirection)
        self.write_int(current_tail_range[RID_COLUMN], tail_rid)
        self.write_int(current_tail_range[TIMESTAMP_COLUMN], int(time()))
        self.write_int(current_tail_range[SCHEMA_ENCODING_COLUMN], schema_encoding_val)
        for i in range(self.num_columns):
            value = columns[i] if columns[i] is not None else self._get_latest_column_value(rid, i, current_indirection, base_page_range, base_record_index)
            self.write_int(current_tail_range[4 + i], value)
        indirection_bytes = tail_rid.to_bytes(8, byteorder='little', signed=True)
        page = self.get_or_load_page(base_page_range[INDIRECTION_COLUMN])
        page.data[base_offset:base_offset+8] = indirection_bytes
        self.bufferpool.mark_dirty(base_page_range[INDIRECTION_COLUMN])
        self.bufferpool.flush_to_disk(base_page_range[INDIRECTION_COLUMN], page)
        tail_page_range_index = len(self.tail_pages) - 1
        tail_record_index = self.get_or_load_page(current_tail_range[0]).num_records - 1
        self.page_directory[tail_rid] = ('tail', tail_page_range_index, tail_record_index)
        for pid in current_tail_range:
            self.bufferpool.flush_to_disk(pid, self.get_or_load_page(pid))
        return True

    def delete_record(self, rid):
        if rid not in self.page_directory:
            return False
        del self.page_directory[rid]
        return True