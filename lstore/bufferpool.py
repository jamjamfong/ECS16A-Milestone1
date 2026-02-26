import os

class BufferPool:
    """
    Tracks dirty pages and handles flushing to disk.
    """

    def __init__(self, db_path, capacity=1024):
        self.db_path = db_path
        self.capacity = capacity
        self.page_registry = {}

    def register_page(self, table_name, page_type, range_idx, col_idx, page):
        key = (table_name, page_type, range_idx, col_idx)
        self.page_registry[key] = page

    def mark_dirty(self, table_name, page_type, range_idx, col_idx):
        key = (table_name, page_type, range_idx, col_idx)
        if key in self.page_registry:
            self.page_registry[key].dirty = True

    def get_dirty_pages(self):
        return {k: v for k, v in self.page_registry.items() if v.dirty}

    # Dirty page to disk
    def flush_page(self, key, page):
        table_name, page_type, range_idx, col_idx = key
        file_path = os.path.join(
            self.db_path,
            f"{table_name}_{page_type}_r{range_idx}_c{col_idx}.bin"
        )
        with open(file_path, 'wb') as f:
            f.write(bytes(page.data))
            f.write(page.num_records.to_bytes(8, byteorder='little'))
        page.dirty = False  # Clean after flush

    # Flush dirty pages on close()/eviction
    def flush_all_dirty(self):
        for key, page in list(self.page_registry.items()):
            if page.dirty:
                self.flush_page(key, page)

    # Load data from disk
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