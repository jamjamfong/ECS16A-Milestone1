import os
from collections import deque

class BufferPool:
    def __init__(self, buffer_size = 100):
        self.buffer_size = buffer_size # Create bufferpool with buffer_size
        self.frames = [None] * self.buffer_size # Each slot holds a page
        self.dirty = [False] * self.buffer_size  # Track whether a frame is dirty
        self.pin_count = [0] * self.buffer_size # Pin count for each frame
        self.page_table = {}
        self.usage_order = deque()
        self.path = ""


    def add_page(self, page_id, page_obj):
        if page_id in self.page_table:
            frame = self.page_table[page_id]
            self.update_usage(frame)
            return frame
        
        for i in range(self.buffer_size):
            if self.frames[i] is None:
                self.insert_to_frame(i, page_id, page_obj)
                return i

        return self.evict_and_add(page_id, page_obj)

    def get_page(self, page_id):
        frame = self.page_table.get(page_id)
        if frame is not None:
            self.update_usage(frame)
            return self.frames[frame]
        return None

    def update_usage(self, frame_index):
        if frame_index in self.usage_order:
            self.usage_order.remove(frame_index)
        self.usage_order.append(frame_index)

    def insert_to_frame(self, frame_idx, page_id, page_obj):
        self.frames[frame_idx] = page_obj
        self.page_table[page_id] = frame_idx
        self.pin_count[frame_idx] = 0
        self.dirty[frame_idx] = False
        self.usage_order.append(frame_idx)

    def evict_and_add(self, page_id, page_obj):
        victim_frame = None
        for frame in self.usage_order:
            if self.pin_count[frame] == 0:
                victim_frame = frame
                break
        if victim_frame is None:
            raise Exception("Bufferpool full: All pages are currently pinned.")
        victim_page_id = None
        for pid, f_idx in self.page_table.items():
            if f_idx == victim_frame:
                victim_page_id = pid
                break

        if self.dirty[victim_frame]:
            self.flush_to_disk(victim_page_id, self.frames[victim_frame])

        del self.page_table[victim_page_id]
        self.usage_order.remove(victim_frame)
        self.insert_to_frame(victim_frame, page_id, page_obj)
        return victim_frame
    
    def flush_to_disk(self, page_id, page_obj):
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        
        file_path = os.path.join(self.path, f"page_{page_id}.bin")

        try:
            with open(file_path, "wb") as f:
                f.write(page_obj.data)
            frame = self.page_table.get(page_id)
            if frame is not None:
                self.dirty[frame] = False
        except IOError as e:
            print(f"Error: Could not write page {page_id} to disk. {e}")

    def open(self, path):
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)

    def close (self):
        for page_id, frame_idx in list(self.page_table.items()):
            if self.dirty[frame_idx]:
                self.flush_to_disk(page_id, self.frames[frame_idx])

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


