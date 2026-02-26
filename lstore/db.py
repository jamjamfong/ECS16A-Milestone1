import os
<<<<<<< HEAD
import io
import json
=======
import struct  # CHANGED: replaced pickle with struct for binary metadata I/O
>>>>>>> 06f21ad53c294d5606cf54bf4a1543648063790b
from lstore.table import Table
from lstore.page import Page
from lstore.bufferpool import BufferPool

class Database():

    def __init__(self):
        self.tables = {}
        self.path = None
        self.bufferpool = None

    def open(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)
        self.bufferpool = BufferPool(path)

<<<<<<< HEAD
        meta_path = os.path.join(path, 'tables.meta')
        if not os.path.exists(meta_path):
            return

        with io.open(meta_path, 'r', encoding='utf-8') as f:
            raw = json.load(f)

        table_metas = []
        for m in raw:
            m['page_directory'] = {int(k): tuple(v) for k, v in m['page_directory'].items()}
            table_metas.append(m)

=======
        meta_path = os.path.join(path, 'tables.meta')  # CHANGED: .pkl -> .meta
        if not os.path.exists(meta_path):
            return

        # CHANGED: replaced pickle.load with binary struct read
        with open(meta_path, 'rb') as f:
            num_tables = struct.unpack('Q', f.read(8))[0]
            table_metas = []
            for _ in range(num_tables):
                name_len = struct.unpack('Q', f.read(8))[0]
                name = f.read(name_len).decode('utf-8')
                num_columns, key, next_rid, num_base_ranges, num_tail_ranges = struct.unpack('5q', f.read(40))
                num_entries = struct.unpack('Q', f.read(8))[0]
                page_directory = {}
                for _ in range(num_entries):
                    rid = struct.unpack('q', f.read(8))[0]
                    ptype_len = struct.unpack('Q', f.read(8))[0]
                    ptype = f.read(ptype_len).decode('utf-8')
                    pr_idx, slot = struct.unpack('2q', f.read(16))
                    page_directory[rid] = (ptype, pr_idx, slot)
                table_metas.append({
                    'name': name,
                    'num_columns': num_columns,
                    'key': key,
                    'next_rid': next_rid,
                    'page_directory': page_directory,
                    'num_base_ranges': num_base_ranges,
                    'num_tail_ranges': num_tail_ranges,
                })
>>>>>>> 06f21ad53c294d5606cf54bf4a1543648063790b

        for meta in table_metas:
            name = meta['name']
            num_columns = meta['num_columns']
            key = meta['key']
            next_rid = meta['next_rid']

            table = Table(name, num_columns, key, self.bufferpool)  # pass bufferpool
            table.next_rid = next_rid
            table.page_directory = meta['page_directory']

<<<<<<< HEAD
            total_columns = 5 + num_columns
=======
            total_columns = 5 + num_columns  # CHANGED: 4 -> 5 to match BASE_RID_COLUMN
>>>>>>> 06f21ad53c294d5606cf54bf4a1543648063790b

            # Load base pages
            base_pages_path = os.path.join(path, f'{name}_base_pages.bin')
            num_base_ranges = meta['num_base_ranges']
            table.base_pages = []
            with open(base_pages_path, 'rb') as f:
                for range_idx in range(num_base_ranges):
                    page_range = []
                    for col_idx in range(total_columns):
                        page = Page()
                        page.data = bytearray(f.read(4096))
                        page.num_records = int.from_bytes(f.read(8), byteorder='little')
                        page.dirty = False  # just loaded from disk, clean
                        page_range.append(page)
                        self.bufferpool.register_page(name, 'base', range_idx, col_idx, page)
                    table.base_pages.append(page_range)

            # Load tail pages
            tail_pages_path = os.path.join(path, f'{name}_tail_pages.bin')
            num_tail_ranges = meta['num_tail_ranges']
            table.tail_pages = []
            with open(tail_pages_path, 'rb') as f:
                for range_idx in range(num_tail_ranges):
                    page_range = []
                    for col_idx in range(total_columns):
                        page = Page()
                        page.data = bytearray(f.read(4096))
                        page.num_records = int.from_bytes(f.read(8), byteorder='little')
                        page.dirty = False  # just loaded from disk, clean
                        page_range.append(page)
                        self.bufferpool.register_page(name, 'tail', range_idx, col_idx, page)
                    table.tail_pages.append(page_range)

            # Rebuild indexes from loaded data
            table.index.indices = [None] * num_columns
            table.index.create_index(key)

            self.tables[name] = table


    def close(self):
        if self.path is None:
            return

<<<<<<< HEAD
        os.makedirs(self.path, exist_ok=True)
        table_metas = []
=======
        # ADDED: wait for any running merge threads before writing to disk
        for table in self.tables.values():
            if table._merge_thread and table._merge_thread.is_alive():
                table._merge_thread.join(timeout=10)

        os.makedirs(self.path, exist_ok=True)
>>>>>>> 06f21ad53c294d5606cf54bf4a1543648063790b

        for name, table in self.tables.items():

            # Only write pages that are dirty
            base_pages_path = os.path.join(self.path, f'{name}_base_pages.bin')
            with open(base_pages_path, 'wb') as f:
                for page_range in table.base_pages:
                    for page in page_range:
                        f.write(bytes(page.data))
                        f.write(page.num_records.to_bytes(8, byteorder='little'))
                        page.dirty = False

            tail_pages_path = os.path.join(self.path, f'{name}_tail_pages.bin')
            with open(tail_pages_path, 'wb') as f:
                for page_range in table.tail_pages:
                    for page in page_range:
                        f.write(bytes(page.data))
                        f.write(page.num_records.to_bytes(8, byteorder='little'))
                        page.dirty = False

<<<<<<< HEAD
            table_metas.append({
                'name': name,
                'num_columns': table.num_columns,
                'key': table.key,
                'next_rid': table.next_rid,
                'page_directory': table.page_directory,
                'num_base_ranges': len(table.base_pages),
                'num_tail_ranges': len(table.tail_pages),
            })

        meta_path = os.path.join(self.path, 'tables.meta')
        with io.open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(table_metas,f)
=======
        # CHANGED: replaced pickle.dump with binary struct write
        meta_path = os.path.join(self.path, 'tables.meta')  # CHANGED: .pkl -> .meta
        with open(meta_path, 'wb') as f:
            f.write(struct.pack('Q', len(self.tables)))
            for name, table in self.tables.items():
                name_bytes = name.encode('utf-8')
                f.write(struct.pack('Q', len(name_bytes)))
                f.write(name_bytes)
                f.write(struct.pack('5q',
                    table.num_columns,
                    table.key,
                    table.next_rid,
                    len(table.base_pages),
                    len(table.tail_pages),
                ))
                f.write(struct.pack('Q', len(table.page_directory)))
                for rid, (ptype, pr_idx, slot) in table.page_directory.items():
                    f.write(struct.pack('q', rid))
                    ptype_bytes = ptype.encode('utf-8')
                    f.write(struct.pack('Q', len(ptype_bytes)))
                    f.write(ptype_bytes)
                    f.write(struct.pack('2q', pr_idx, slot))
>>>>>>> 06f21ad53c294d5606cf54bf4a1543648063790b

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            return self.tables[name]
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