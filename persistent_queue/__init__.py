"""
An implementation of a persistent queue. It is optimized for peeking at values
and then deleting them off to top of the queue.
"""

import os.path
import pickle
import shutil
import struct
import threading
import uuid

LENGTH_STRUCT = 'I'
HEADER_STRUCT = 'II'
START_OFFSET = 4 + 4


class PersistentQueue:
    def __init__(self, filename, path='.', flush_limit=1048576):
        self.filename = filename
        self.path = path
        self.file = self._open_file()
        self.flush_limit = flush_limit
        self.lock = threading.RLock()

        self.file.seek(0, 0)
        self.length = struct.unpack(HEADER_STRUCT[0], self.file.read(4))[0]

    def _open_file(self, mode=None):
        filename = os.path.join(self.path, self.filename)

        mode = mode or 'r+b' if os.path.isfile(filename) else 'w+b'
        file = open(filename, mode=mode, buffering=0)

        if mode == 'w+b':
            # write length and start pointer
            file.write(struct.pack(HEADER_STRUCT, 0, START_OFFSET))

        return file

    def _update_length(self, length):
        current_pos = self.file.tell()

        self.file.seek(0, 0)  # Go to the beginning of the file
        self.file.write(struct.pack(HEADER_STRUCT[0], length))
        self.file.flush()  # Probably not necessary since buffering=0
        os.fsync(self.file.fileno())

        self.length = length

        self.file.seek(current_pos, 0)

    def _get_queue_top(self):
        current_pos = self.file.tell()

        self.file.seek(START_OFFSET - 4, 0)  # Start at beginning of file
        pos = struct.unpack(HEADER_STRUCT[1], self.file.read(4))[0]

        self.file.seek(current_pos, 0)
        return pos

    def _set_queue_top(self, top):
        current_pos = self.file.tell()

        self.file.seek(START_OFFSET - 4, 0)  # Start at beginning of file
        self.file.write(struct.pack(HEADER_STRUCT[1], top))
        self.file.flush()  # Probably not necessary since buffering=0
        os.fsync(self.file.fileno())

        self.file.seek(current_pos, 0)

    def clear(self):
        """Removes all elements from queue."""
        with self.lock:
            self.file.close()
            self.file = self._open_file(mode='w+b')
            self.length = 0

    def copy(self, new_filename, path=None):
        """Copies a queue to a new queue."""
        old = os.path.join(self.path, self.filename)
        new = os.path.join(path or self.path, new_filename)
        shutil.copy2(old, new)

        return PersistentQueue(filename=new_filename,
                               path=path or self.path,
                               flush_limit=self.flush_limit)

    def count(self):
        """Return the size of the queue."""
        return self.length

    def flush(self):
        """
        Removes elements that have been deleted or popped from the queue. This
        will be taken care of by pop and delete.
        """
        with self.lock:
            pos = self._get_queue_top()

        if pos < self.flush_limit:
            # Ignore if the file isn't big enough -- it's not worth it
            return

        # Make a new file
        random = str(uuid.uuid4()).replace('-', '')
        temp_filename = os.path.join(self.path, self.filename + '-' + random)
        new_file = open(temp_filename, mode='w+b', buffering=0)

        # From this point on, the file can not change
        with self.lock:
            # Make sure everything is to disk
            self.file.flush()
            os.fsync(self.file.fileno())

            start = self._get_queue_top()  # Get it again in case it changed
            self.file.seek(0, 2)  # Go to end of file
            end = self.file.tell()

            # Copy over meta data
            new_file.write(struct.pack(HEADER_STRUCT,
                                       self.count(),
                                       START_OFFSET))

            # Copy over data
            # Do it in chunks so we aren't loading tons of data into memory
            self.file.seek(start, 0)
            bytes_read = 0
            chunk_size = 4096
            while bytes_read < end - start:
                bytes_read += chunk_size
                new_file.write(self.file.read(bytes_read))

            new_file.flush()  # Probably not necessary since buffering=0
            os.fsync(new_file.fileno())
            new_file.close()
            self.file.close()

            # So far everything above this point has been safe. If something
            # crashed, the data would still be preserved. Now we are entering
            # the danger zone.

            os.replace(temp_filename, os.path.join(self.path, self.filename))
            self.file = self._open_file()

    def pop(self, items=1):
        """
        Removes and returns a certain amount of items from the queue. If items
        is greater than one, a list is returned.
        """
        with self.lock:
            data = self.peek(items)
            self._set_queue_top(self.file.tell())

            if isinstance(data, list):
                if len(data) > 0:
                    self._update_length(self.count() - len(data))
            elif data is not None:
                self._update_length(self.count() - 1)

            # I don't want to block sending data back, so start a thread
            threading.Thread(target=self.flush)

            return data

    def peek(self, items=1):
        """
        Returns a certain amount of items from the queue. If items is greater
        than one, a list is returned.
        """
        def read_data():
            length = struct.unpack(LENGTH_STRUCT, self.file.read(4))[0]
            data = self.file.read(length)
            return pickle.loads(data)

        with self.lock:
            self.file.seek(self._get_queue_top(), 0)  # Beginning of data
            total_items = self.count() if items > self.count() else items
            data = [read_data() for i in range(total_items)]

        if items == 1:
            if len(data) == 0:
                return None
            else:
                return data[0]
        else:
            return data

    def delete(self, items=1):
        """Removes items from queue. Nothing is returned."""
        def read_length():
            length = struct.unpack(LENGTH_STRUCT, self.file.read(4))[0]
            self.file.seek(length, 1)

        with self.lock:
            self.file.seek(self._get_queue_top(), 0)  # Beginning of data
            total_items = self.count() if items > self.count() else items

            for _ in range(total_items):
                read_length()

            self._set_queue_top(self.file.tell())
            self._update_length(self.count() - total_items)

            # I don't want to block sending data back, so start a thread
            threading.Thread(target=self.flush)

    def push(self, items):
        """Add items to the queue."""
        def write_data(item):
            data = pickle.dumps(item)
            self.file.write(struct.pack(LENGTH_STRUCT, len(data)))
            self.file.write(data)
            self.file.flush()  # Probably not necessary since buffering=0
            os.fsync(self.file.fileno())

        if not isinstance(items, list):
            items = [items]

        with self.lock:
            self.file.seek(0, 2)  # Go to end of file

            for i in items:
                write_data(i)

            self._update_length(self.count() + len(items))

    def __len__(self):
        """Get size of queue."""
        return self.count()
