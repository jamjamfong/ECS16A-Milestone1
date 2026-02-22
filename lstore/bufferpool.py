class BufferPool:
    def __init__(self, buffer_size = 100):
        self.buffer_size = buffer_size # Create bufferpool with buffer_size
        self.frames = [None] * self.buffer_size # Each slot holds a page
        self.dirty = [False] * self.buffer_size  # Track whether a frame is dirty
        self.pin_count = [0] * self.buffer_size # Pin count for each frame
        self.page_table = {}

# Add page to bufferpool
    def add_page(self, page_id, page):
        # If page already in bufferpool return page
        if page_id in self.page_table:
            return self.page_table[page_id]
        
        # Find empty spot for frame
        for i in range(self.buffer_size):
            if self.frames[i] is None:
                self.frames[i] = page
                self.page_table[page_id] = i
                self.pin_count[i] = 0
                self.dirty[i] = False
                return i

        raise Exception("BufferPool full. Replacement policy not implemented.")

    def get_page(self, page_id):
        frame = self.page_table.get(page_id)
        if frame is None:
            return None
        return self.frames[frame]

    def pin(self, page_id):
        frame = self.page_table.get(page_id)
        if frame is not None:
            self.pin_count[frame] += 1

    def unpin(self, page_id):
        frame = self.page_table.get(page_id)
        if frame is not None and self.pin_count[frame] > 0:
            self.pin_count[frame] -= 1

    def mark_dirty(self, page_id):
        frame = self.page_table.get(page_id)
        if frame is not None:
            self.dirty[frame] = True

    def is_dirty(self, page_id):
        frame = self.page_table.get(page_id)
        if frame is None:
            return False
        return self.dirty[frame]


