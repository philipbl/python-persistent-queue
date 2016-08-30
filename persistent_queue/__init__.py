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
            file.write(struct.pack(HEADER_STRUCT, 0, START_OFFSET))

        return file

    # TODO: Protect this method
    def _write_data(self, item):
        data = pickle.dumps(item)
        self.file.write(struct.pack(LENGTH_STRUCT, len(data)))
        self.file.write(data)

    # TODO: Protect this method
    def _read_data(self):
        print("Reading data:", self.file.tell())
        length = struct.unpack(LENGTH_STRUCT, self.file.read(4))[0]
        data = self.file.read(length)
        return pickle.loads(data)

    @return_file_position
    def _update_length(self, length):
        self.file.seek(0, 0)  # Go to the beginning of the file
        self.file.write(struct.pack(HEADER_STRUCT[0], length))

    @return_file_position
    def count(self):
        self.file.seek(0, 0)  # Start at beginning of file
        length = struct.unpack(HEADER_STRUCT[0], self.file.read(4))[0]
        return length

    @return_file_position
    def _get_queue_top(self):
        self.file.seek(START_OFFSET - 4, 0)  # Start at beginning of file
        pos = struct.unpack(HEADER_STRUCT[1], self.file.read(4))[0]
        return pos

    @return_file_position
    def _set_queue_top(self, top):
        self.file.seek(START_OFFSET - 4, 0)  # Start at beginning of file
        self.file.write(struct.pack(HEADER_STRUCT[1], top))

    def flush(self):
        pass

    def push(self, items):
        if not isinstance(items, list):
            items = [items]

        self.file.seek(0, 2)  # Go to end of file

        for i in items:
            print(i)
            self._write_data(i)

        self._update_length(self.count() + len(items))

    def clear(self):
        self.file.close()
        self.file = self._open_file(mode='w+b')

    def pop(self, items=1):
        data = self.peek(items)
        self._set_queue_top(self.file.tell())

        if isinstance(data, list):
            if len(data) > 0:
                self._update_length(self.count() - len(data))
        elif data is not None:
            self._update_length(self.count() - 1)

        return data

    def peek(self, items=1):
        self.file.seek(self._get_queue_top(), 0)  # Start at beginning of data

        length = self.count()
        total_items = length if items > length else items

        data = [self._read_data() for i in range(total_items)]

        if items == 1:
            if len(data) == 0:
                return None
            else:
                return data[0]
        else:
            return data

    def __len__(self):
        return self.count()
