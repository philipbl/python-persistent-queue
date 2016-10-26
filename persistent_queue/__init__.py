"""
An implementation of a persistent queue. It is optimized for peeking at values
and then deleting them off to top of the queue.
"""

import logging
import os.path
import pickle
import shutil
import struct
import threading
import uuid

LENGTH_STRUCT = 'I'
HEADER_STRUCT = 'II'
START_OFFSET = 4 + 4

_LOGGER = logging.getLogger(__name__)


class PersistentQueue:
    def __init__(self, filename, path='.', dumps=pickle.dumps,
                 loads=pickle.loads, flush_limit=1048576):
        self.filename = filename
        self.path = path
        self.file = self._open_file()
        self.loads = loads
        self.dumps = dumps
        self.flush_limit = flush_limit
        self.file_lock = threading.RLock()
        self.pop_lock = threading.RLock()
        self.pushed_event = threading.Event()

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
        _LOGGER.debug("Clearing the queue")
        with self.file_lock, self.pop_lock:
            self.file.close()
            self.file = self._open_file(mode='w+b')
            self.length = 0
            _LOGGER.debug("The queue has been cleared")

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
        _LOGGER.debug("Flushing the queue")

        with self.file_lock:
            pos = self._get_queue_top()

        if pos < self.flush_limit:
            # Ignore if the file isn't big enough -- it's not worth it
            _LOGGER.debug("Ignoring flush because we haven't met the limit")
            return

        # Make a new file
        random = str(uuid.uuid4()).replace('-', '')
        temp_filename = os.path.join(self.path, self.filename + '-' + random)
        new_file = open(temp_filename, mode='w+b', buffering=0)

        # From this point on, the file can not change
        with self.file_lock, self.pop_lock:
            # Make sure everything is to disk
            self.file.flush()
            os.fsync(self.file.fileno())

            start = self._get_queue_top()  # Get it again in case it changed
            self.file.seek(0, 2)  # Go to end of file
            end = self.file.tell()

            _LOGGER.debug("Writing data to new file")
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

            _LOGGER.debug("Replacing old file with new file")
            os.replace(temp_filename, os.path.join(self.path, self.filename))
            self.file = self._open_file()

            _LOGGER.debug("Finished flushing the queue")

    def pop(self, items=1, blocking=False):
        """
        Removes and returns a certain amount of items from the queue. If items
        is greater than one, a list is returned.
        """
        _LOGGER.debug("Popping %s items", items)

        # Ignore requests for zero items
        if items == 0:
            _LOGGER.debug("Returning empty list")
            return []

        with self.pop_lock:
            data, queue_top = self._peek(items, blocking)

            with self.file_lock:
                self._set_queue_top(queue_top)

                if isinstance(data, list):
                    if len(data) > 0:
                        self._update_length(self.count() - len(data))
                elif data is not None:
                    self._update_length(self.count() - 1)

                _LOGGER.debug("Returning data from the pop")
                return data

    def _peek(self, items=1, blocking=False):
        """
        Returns a certain amount of items from the queue. If items is greater
        than one, a list is returned.
        """
        def read_data():
            length = struct.unpack(LENGTH_STRUCT, self.file.read(4))[0]
            data = self.file.read(length)
            return self.loads(data)

        _LOGGER.debug("Peeking %s items", items)

        # Ignore requests for zero items
        if items == 0:
            _LOGGER.debug("Returning empty list")
            return [], self.file.tell()

        if blocking:
            while self.count() < items:
                self.pushed_event.wait()

        with self.file_lock:
            self.file.seek(self._get_queue_top(), 0)  # Beginning of data
            total_items = self.count() if items > self.count() else items
            data = [read_data() for i in range(total_items)]
            queue_top = self.file.tell()

        if items == 1:
            if len(data) == 0:
                _LOGGER.debug("No items to peek at so returning None")
                return None, queue_top
            else:
                _LOGGER.debug("Returning data from peek")
                return data[0], queue_top
        else:
            _LOGGER.debug("Returning data from peek")
            return data, queue_top

    def peek(self, items=1, blocking=False):
        with self.pop_lock:
            return self._peek(items, blocking)[0]

    def delete(self, items=1):
        """Removes items from queue. Nothing is returned."""
        def read_length():
            length = struct.unpack(LENGTH_STRUCT, self.file.read(4))[0]
            self.file.seek(length, 1)

        _LOGGER.debug("Deleting %s items", items)

        # Ignore requests for zero items
        if items == 0:
            _LOGGER.debug("Ignoring request to delete")
            return

        with self.file_lock, self.pop_lock:
            self.file.seek(self._get_queue_top(), 0)  # Beginning of data
            total_items = self.count() if items > self.count() else items

            for _ in range(total_items):
                read_length()

            self._set_queue_top(self.file.tell())
            self._update_length(self.count() - total_items)

        _LOGGER.debug("Done deleting data")

    def push(self, items):
        """Add items to the queue."""
        def write_data(item):
            data = self.dumps(item)
            self.file.write(struct.pack(LENGTH_STRUCT, len(data)))
            self.file.write(data)
            self.file.flush()  # Probably not necessary since buffering=0
            os.fsync(self.file.fileno())

        if not isinstance(items, list):
            items = [items]

        _LOGGER.debug("Pushing %s items", len(items))

        # Ignore requests for adding zero items
        if len(items) == 0:
            _LOGGER.debug("Pushing zero items, ignoring request")
            return

        with self.file_lock:
            self.file.seek(0, 2)  # Go to end of file

            for i in items:
                write_data(i)

            self._update_length(self.count() + len(items))

        self.pushed_event.set()
        _LOGGER.debug("Done pushing data")

    def __len__(self):
        """Get size of queue."""
        return self.count()
