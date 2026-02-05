
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        # Each record is 8 bytes (assuming integer size), so we can store 4096 / 8 = 512 records
        # Checks if the current number of records is less than the maximum capacity
        return self.num_records < (len(self.data) // 8)

    def write(self, value):
        """
        Store an integer value in the page
        Converts the integer to bytes and stores it in the bytearray at the correct position
        """
        self.num_records += 1
        offset = (self.num_records - 1) * 8
        self.data[offset:offset + 8] = value.to_bytes(8, byteorder ='little', signed=True)

