import os
<<<<<<< HEAD
=======
from collections import OrderedDict  # ADDED
>>>>>>> 06f21ad53c294d5606cf54bf4a1543648063790b

class BufferPool:
    """
    Tracks dirty pages and handles flushing to disk.
    """

    def __init__(self, db_path, capacity=1024):
        self.db_path = db_path
        self.capacity = capacity
<<<<<<< HEAD
        self.page_registry = {}

    def register_page(self, table_name, page_type, range_idx, col_idx, page):
        key = (table_name, page_type, range_idx, col_idx)
        self.page_registry[key] = page
=======
        self.page_registry = OrderedDict()  

    def register_page(self, table_name, page_type, range_idx, col_idx, page):
        key = (table_name, page_type, range_idx, col_idx)
        if key not in self.page_registry and len(self.page_registry) >= self.capacity:
            self._evict_one()
        self.page_registry[key] = page
        self.page_registry.move_to_end(key)  

    def _evict_one(self):
        for key, page in self.page_registry.items():
            if page.pin_count == 0:
                if page.dirty:
                    self.flush_page(key, page)
                del self.page_registry[key]
                return
        raise MemoryError("BufferPool: all pages are pinned, cannot evict")
>>>>>>> 06f21ad53c294d5606cf54bf4a1543648063790b

    def mark_dirty(self, table_name, page_type, range_idx, col_idx):
        key = (table_name, page_type, range_idx, col_idx)
        if key in self.page_registry:
            self.page_registry[key].dirty = True

    def get_dirty_pages(self):
        return {k: v for k, v in self.page_registry.items() if v.dirty}

<<<<<<< HEAD
    # Dirty page to disk
=======
>>>>>>> 06f21ad53c294d5606cf54bf4a1543648063790b
    def flush_page(self, key, page):
        table_name, page_type, range_idx, col_idx = key
        file_path = os.path.join(
            self.db_path,
            f"{table_name}_{page_type}_r{range_idx}_c{col_idx}.bin"
        )
        with open(file_path, 'wb') as f:
            f.write(bytes(page.data))
            f.write(page.num_records.to_bytes(8, byteorder='little'))
<<<<<<< HEAD
        page.dirty = False  # Clean after flush

    # Flush dirty pages on close()/eviction
=======
        page.dirty = False

>>>>>>> 06f21ad53c294d5606cf54bf4a1543648063790b
    def flush_all_dirty(self):
        for key, page in list(self.page_registry.items()):
            if page.dirty:
                self.flush_page(key, page)
<<<<<<< HEAD

    # Load data from disk
=======
 
>>>>>>> 06f21ad53c294d5606cf54bf4a1543648063790b
    def load_page(self, table_name, page_type, range_idx, col_idx, page):
        file_path = os.path.join(
            self.db_path,
            f"{table_name}_{page_type}_r{range_idx}_c{col_idx}.bin"
        )
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                page.data = bytearray(f.read(4096))
                page.num_records = int.from_bytes(f.read(8), byteorder='little')
            page.dirty = False