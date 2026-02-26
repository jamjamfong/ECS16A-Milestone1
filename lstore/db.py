import os
import io
import json
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

        meta_path = os.path.join(path, 'tables.meta')
        if not os.path.exists(meta_path):
            return

        with io.open(meta_path, 'r', encoding='utf-8') as f:
            raw = json.load(f)

        table_metas = []
        for m in raw:
            m['page_directory'] = {int(k): tuple(v) for k, v in m['page_directory'].items()}
            table_metas.append(m)


        for meta in table_metas:
            name = meta['name']
            num_columns = meta['num_columns']
            key = meta['key']
            next_rid = meta['next_rid']

            table = Table(name, num_columns, key, self.bufferpool)  # pass bufferpool
            table.next_rid = next_rid
            table.page_directory = meta['page_directory']

            total_columns = 5 + num_columns

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

        os.makedirs(self.path, exist_ok=True)
        table_metas = []

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