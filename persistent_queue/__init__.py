from functools import wraps
import os.path
import pickle
import struct

LENGTH_STRUCT = 'I'
HEADER_STRUCT = 'II'
START_OFFSET = 4 + 4

def return_file_position(f):
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        current_pos = self.file.tell()
        r = f(self, *args, **kwargs)
        self.file.seek(current_pos, 0)

        return r
    return wrapped

class PersistentQueue:
    def __init__(self, filename=None, path='.'):
        self.filename = filename or "foobar"
        self.path = path
        self.file = self._open_file()

    def _open_file(self, mode=None):
        filename = os.path.join(self.path, self.filename)

        mode = mode or 'r+b' if os.path.isfile(filename) else 'w+b'
        file = open(filename, mode=mode, buffering=0)

        if mode == 'w+b':
            # write length and start pointer
            file.write(struct.pack(HEADER_STRUCT, 0, 0))

        return file

    # TODO: Protect this method
    def write_data(self, item):
        data = pickle.dumps(item)
        self.file.write(struct.pack(LENGTH_STRUCT, len(data)))
        self.file.write(data)

    # TODO: Protect this method
    def read_data(self):
        print("Reading data:", self.file.tell())
        length = struct.unpack(LENGTH_STRUCT, self.file.read(4))[0]
        data = self.file.read(length)
        return pickle.loads(data)

    # TODO: Make this private
    def get_data(self, start, end):
        self.file.seek(START_OFFSET, 0)  # Start at beginning of file

        if start is None:
            start = 0

        print("-" * 80)
        print("start:", start)
        print("end:", end)

        # TODO: Make sure start and end are not greater than len
        for i in range(start):
            print(i)
            length = struct.unpack(LENGTH_STRUCT, self.file.read(4))[0]
            self.file.seek(length, 1)

        if end is None:
            print("setting end to count")
            end = self.count()

        print("start:", start)
        print("end:", end)

        items = []
        for i in range(end - start):
            items.append(self.read_data())

        print("-" * 80)
        return items

    @return_file_position
    def update_length(self, length):
        self.file.seek(0, 0)  # Go to the beginning of the file
        self.file.write(struct.pack(HEADER_STRUCT[0], length))

    def push(self, items):
        if not isinstance(items, list):
            items = [items]

        self.file.seek(0, 2)  # Go to end of file

        for i in items:
            print(i)
            self.write_data(i)

        self.update_length(self.count() + len(items))

    def clear(self):
        self.file.close()
        self.file = self._open_file(mode='w+b')

    def pop(self, items=1):
        pass

    def peek(self, items=1):
        self.file.seek(START_OFFSET, 0)  # Start at beginning of file

        length = self.count()
        items = length if items > length else items

        data = [self.read_data() for i in range(items)]

        if items == 1:
            return data[0]
        else:
            return data

    @return_file_position
    def count(self):
        self.file.seek(0, 0)  # Start at beginning of file
        length = struct.unpack(HEADER_STRUCT[0], self.file.read(4))[0]
        return length

    def __len__(self):
        return self.count()
