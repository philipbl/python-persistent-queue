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

try:
    import queue
except ImportError:  # pragma: no cover
    import Queue as queue

LENGTH_STRUCT = 'I'
HEADER_STRUCT = 'II'
START_OFFSET = 4 + 4

_LOGGER = logging.getLogger(__name__)


class PersistentQueue:
    def __init__(self, filename, maxsize=0, dumps=pickle.dumps, loads=pickle.loads, flush_limit=1048576):
        """
        Creates a new PersistentQueue object and underlying file.

        filename: must be a full path to the new file.
        maxsize: upperbound limit of items that can be placed in the queue.
        dumps: the function called for persisting queue items to the file.
        loads: the function called for loading queue items from the file.
        flush_limit: below this filesize, flush() is a no-op.
        """
        if maxsize < 0:
            maxsize = 0

        self.maxsize = maxsize
        self.filename = os.path.abspath(filename)
        self.dumps = dumps
        self.loads = loads
        self.flush_limit = flush_limit

        self._file = self._open_file()
        self._file_lock = threading.RLock()
        self._get_lock = threading.RLock()
        self._get_event = threading.Event()
        self._put_lock = threading.RLock()
        self._put_event = threading.Event()

        self._all_tasks_done = threading.Condition()
        self._unfinished_tasks = 0

        self._file.seek(0, 0)
        self._length = struct.unpack(HEADER_STRUCT[0], self._file.read(4))[0]

    def _open_file(self, mode=None):
        mode = mode or 'r+b' if os.path.isfile(self.filename) else 'w+b'
        file = open(self.filename, mode=mode, buffering=0)

        if mode == 'w+b':
            # write length and start pointer
            file.write(struct.pack(HEADER_STRUCT, 0, START_OFFSET))

        return file

    def _update_length(self, length):
        current_pos = self._file.tell()

        self._file.seek(0, 0)  # Go to the beginning of the file
        self._file.write(struct.pack(HEADER_STRUCT[0], length))
        self._file.flush()  # Probably not necessary since buffering=0
        os.fsync(self._file.fileno())

        self._length = length

        self._file.seek(current_pos, 0)

    def _get_queue_top(self):
        current_pos = self._file.tell()

        self._file.seek(START_OFFSET - 4, 0)  # Start at beginning of file
        pos = struct.unpack(HEADER_STRUCT[1], self._file.read(4))[0]

        self._file.seek(current_pos, 0)
        return pos

    def _set_queue_top(self, top):
        current_pos = self._file.tell()

        self._file.seek(START_OFFSET - 4, 0)  # Start at beginning of file
        self._file.write(struct.pack(HEADER_STRUCT[1], top))
        self._file.flush()  # Probably not necessary since buffering=0
        os.fsync(self._file.fileno())

        self._file.seek(current_pos, 0)

    def _peek(self, block=False, timeout=None):
        with self._get_lock:
            _LOGGER.debug("Peeking item")
            self._put_event.clear()

            if self._length < 1:
                if not block:
                    raise queue.Empty

                # Wait for something to be added
                if self._put_event.wait(timeout) is False:
                    # Nothing was added to the queue and timeout expired
                    # This will never happen if timeout is None
                    raise queue.Empty

                self._put_event.clear()

            with self._file_lock:
                self._file.seek(self._get_queue_top(), 0)  # Beginning of data
                length = struct.unpack(LENGTH_STRUCT, self._file.read(4))[0]
                data = self._file.read(length)
                item = self.loads(data)

                queue_top = self._file.tell()

                return item, queue_top

    def qsize(self):
        """
        Provides compatibility with stdlib Queue objects.

        Return the approximate size of the queue. Note, qsize() > 0 doesn't
        guarantee that a subsequent get() will not block, nor will qsize() <
        maxsize guarantee that put() will not block.
        """
        return self._length

    def empty(self):
        """
        Provides compatibility with stdlib Queue objects.

        Return True if the queue is empty, False otherwise. If empty() returns
        True it doesn't guarantee that a subsequent call to put() will not
        block. Similarly, if empty() returns False it doesn't guarantee that a
        subsequent call to get() will not block.
        """
        return self._length == 0

    def full(self):
        """
        Provides compatibility with stdlib Queue objects.

        Return True if the queue is full, False otherwise. If full() returns
        True it doesn't guarantee that a subsequent call to get() will not
        block. Similarly, if full() returns False it doesn't guarantee that a
        subsequent call to put() will not block.
        """
        return self.maxsize > 0 and self._length >= self.maxsize

    def put(self, item, block=True, timeout=None):
        """
        Provides compatibility with stdlib Queue objects.
        When this function returns, item is guaranteed to be persisted
        into the file and the underlying storage.

        Put item into the queue. If optional args block is true and timeout is
        None (the default), block if necessary until a free slot is available.
        If timeout is a positive number, it blocks at most timeout seconds and
        raises the Full exception if no free slot was available within that
        time. Otherwise (block is false), put an item on the queue if a free
        slot is immediately available, else raise the Full exception (timeout
        is ignored in that case).
        """

        _LOGGER.debug("Putting item")

        with self._put_lock:
            self._get_event.clear()
            if self.maxsize > 0 and self._length + 1 > self.maxsize:
                if not block:
                    raise queue.Full

                if self._get_event.wait(timeout) is False:
                    # Nothing was removed from the queue and timeout expired
                    # This will never happen if timeout is None
                    raise queue.Full
                self._get_event.clear()

            # Convert the object outside of the file lock
            data = self.dumps(item)

            with self._file_lock:
                self._file.seek(0, 2)  # Go to end of file
                self._file.write(struct.pack(LENGTH_STRUCT, len(data)))
                self._file.write(data)
                self._file.flush()  # Probably not necessary since buffering=0
                os.fsync(self._file.fileno())

                self._update_length(self._length + 1)
                self._unfinished_tasks += 1

            self._put_event.set()
            _LOGGER.debug("Done putting data")

    def put_nowait(self, item):
        """
        Provides compatibility with stdlib Queue objects.

        Equivalent to put(item, False).
        """
        self.put(item, block=False)

    def get(self, block=True, timeout=None):
        """
        Provides compatibility with stdlib Queue objects.

        Remove and return an item from the queue. If optional args block is
        true and timeout is None (the default), block if necessary until an
        item is available. If timeout is a positive number, it blocks at most
        timeout seconds and raises the Empty exception if no item was available
        within that time. Otherwise (block is false), return an item if one is
        immediately available, else raise the Empty exception (timeout is
        ignored in that case).
        """
        _LOGGER.debug("Getting item")

        with self._get_lock:
            data, queue_top = self._peek(block, timeout)

            with self._file_lock:
                self._set_queue_top(queue_top)
                self._update_length(self._length - 1)
                self._get_event.set()
                _LOGGER.debug("Returning data from get")
                return data

    def get_nowait(self):
        """
        Provides compatibility with stdlib Queue objects.

        Equivalent to get(False).

        Two methods are offered to support tracking whether enqueued tasks
        have been fully processed by daemon consumer threads.
        """
        return self.get(block=False)

    def task_done(self):
        """
        Provides compatibility with stdlib Queue objects.
        Taken from the Python 3.5 stdlib: lib/python3.5/queue.py

        Indicate that a formerly enqueued task is complete. Used by queue
        consumer threads. For each get() used to fetch a task, a subsequent
        call to task_done() tells the queue that the processing on the task is
        complete.

        If a join() is currently blocking, it will resume when all items have
        been processed (meaning that a task_done() call was received for every
        item that had been put() into the queue).

        Raises a ValueError if called more times than there were items placed in the queue.
        """
        with self._all_tasks_done:
            unfinished = self._unfinished_tasks - 1
            if unfinished < 0:
                raise ValueError('task_done() called too many times')
            if unfinished == 0:
                self._all_tasks_done.notify_all()
            self._unfinished_tasks = unfinished

    def join(self):
        """
        Provides compatibility with stdlib Queue objects.
        Taken from the Python 3.5 stdlib: lib/python3.5/queue.py

        Blocks until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer thread calls task_done()
        to indicate that the item was retrieved and all work on it is complete.
        When the count of unfinished tasks drops to zero, join() unblocks.
        """
        with self._all_tasks_done:
            while self._unfinished_tasks:
                self._all_tasks_done.wait()

    def peek(self, block=False, timeout=None):
        """
        Peeks into the queue and returns an item without removing it.
        """
        item, _ = self._peek(block, timeout)
        return item

    def clear(self):
        """
        Removes all elements from queue, by truncating the file and reloading.
        """
        _LOGGER.debug("Clearing the queue")
        with self._file_lock, self._get_lock:
            self._file.close()
            self._file = self._open_file(mode='w+b')
            self._length = 0
            _LOGGER.debug("The queue has been cleared")

    def copy(self, new_filename):
        """
        Copies a queue to a new queue by duplicating the underlying file.

        new_filename: must be a full path to the new file.
        """
        shutil.copy2(self.filename, new_filename)
        return PersistentQueue(maxsize=self.maxsize,
                               filename=new_filename,
                               dumps=self.dumps,
                               loads=self.loads,
                               flush_limit=self.flush_limit)

    def flush(self):
        """
        Removes elements that have been deleted or gotten from the queue.
        """
        _LOGGER.debug("Flushing the queue")

        with self._file_lock:
            pos = self._get_queue_top()

        if pos < self.flush_limit:
            # Ignore if the file isn't big enough -- it's not worth it
            _LOGGER.debug("Ignoring flush because we haven't met the limit")
            return

        # Make a new file
        random = str(uuid.uuid4()).replace('-', '')
        temp_filename = self.filename + '-' + random
        new_file = open(temp_filename, mode='w+b', buffering=0)

        # From this point on, the file can not change
        with self._file_lock, self._get_lock:
            # Make sure everything is to disk
            self._file.flush()
            os.fsync(self._file.fileno())

            start = self._get_queue_top()  # Get it again in case it changed
            self._file.seek(0, 2)  # Go to end of file
            end = self._file.tell()

            _LOGGER.debug("Writing data to new file")
            # Copy over meta data
            new_file.write(struct.pack(HEADER_STRUCT,
                                       self._length,
                                       START_OFFSET))

            # Copy over data
            # Do it in chunks so we aren't loading tons of data into memory
            self._file.seek(start, 0)
            bytes_read = 0
            chunk_size = 4096
            while bytes_read < end - start:
                bytes_read += chunk_size
                new_file.write(self._file.read(bytes_read))

            new_file.flush()  # Probably not necessary since buffering=0
            os.fsync(new_file.fileno())
            new_file.close()
            self._file.close()

            # So far everything above this point has been safe. If something
            # crashed, the data would still be preserved. Now we are entering
            # the danger zone.

            _LOGGER.debug("Replacing old file with new file")
            os.remove(self.filename)
            os.rename(temp_filename, self.filename)
            self._file = self._open_file()

            _LOGGER.debug("Finished flushing the queue")

    def delete(self):
        """
        Remove item from queue without reading object from file.
        """
        _LOGGER.debug("Deleting item")

        # If there is nothing in the queue, do nothing
        if self._length < 1:
            return

        with self._file_lock, self._get_lock:
            self._file.seek(self._get_queue_top(), 0)  # Beginning of data
            length = struct.unpack(LENGTH_STRUCT, self._file.read(4))[0]
            self._file.seek(length, 1)
            self._set_queue_top(self._file.tell())
            self._update_length(self._length - 1)

        _LOGGER.debug("Done deleting data")

    def __len__(self):
        """
        Get size of queue.
        """
        return self._length

    def __repr__(self):
        """
        Return a representation of a persistent queue.
        """
        string = "PersistentQueue(filename={filename!r}, " + \
                 "maxsize={maxsize!r}, " + \
                 "dumps={dumps}, " + \
                 "loads={loads}, " + \
                 "flush_limit={flush_limit!r})"

        return string.format(filename=self.filename,
                             maxsize=self.maxsize,
                             dumps=self.dumps,
                             loads=self.loads,
                             flush_limit=self.flush_limit)
