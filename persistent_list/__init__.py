import os.path
import pickle
import struct

LENGTH_STRUCT = 'I'
HEADER_STRUCT = 'II'
START_OFFSET = 4 + 4

class PersistentList:
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
        length = struct.unpack(LENGTH_STRUCT, self.file.read(4))[0]
        data = self.file.read(length)
        return pickle.loads(data)

    # TODO: Make this private
    def get_data(self, start, end):
        self.file.seek(START_OFFSET, 0)  # Start at beginning of file

        print("start:", start)
        print("end:", end)

        # TODO: Make sure start and end are not greater than len
        for i in range(start):
            print(i)
            length = struct.unpack(LENGTH_STRUCT, self.file.read(4))[0]
            self.file.seek(length, 1)

        if end is None:
            end = self.count()

        items = []
        for i in range(end - start):
            items.append(self.read_data())

        return items

    def update_length(self, length):
        self.file.seek(0, 0)  # Go to the beginning of the file
        self.file.write(struct.pack(HEADER_STRUCT[0], length))

    def append(self, item):
        self.file.seek(0, 2)  # Go to end of file
        self.write_data(item)
        self.update_length(self.count() + 1)

    def clear(self):
        self.file.close()
        self.file = self._open_file(mode='w+b')

    def copy(self):
        pass

    def count(self):
        self.file.seek(0, 0)  # Start at beginning of file
        return struct.unpack(HEADER_STRUCT[0], self.file.read(4))[0]

    def __del__(self):
        pass

    def extend(self, lst):
        for item in lst:
            self.append(item)

    def __getitem__(self, index):
        if not isinstance(index, int) and not isinstance(index, slice):
            raise TypeError("PersistentList indices must be integers or slices, not {}".format(type(index)))

        if isinstance(index, int):
            return self.get_data(index, index + 1)[0]
        else:
            # TODO: Add support for step!
            # TODO: Add support for higher stop than size
            print(index)
            return self.get_data(index.start or 0, index.stop)

    def index(self, item):
        pass

    def insert(self, item, index):
        pass

    def __len__(self):
        return self.count()

    def pop(self, index):
        pass

    def remove(self, item):
        pass

    def reverse(self):
        pass
