import os
import random
import threading
import time
import uuid
import pytest

try:
    import queue
except ImportError:
    import Queue as queue

from persistent_queue import PersistentQueue


@pytest.fixture(autouse=True)
def t(tmpdir):
    os.chdir(str(tmpdir))


class TestPersistentQueue:
    def setup_method(self):
        random = str(uuid.uuid4()).replace('-', '')
        filename = '{}_{}.queue'.format(self.__class__.__name__, random)
        self.queue = PersistentQueue(filename)

    def teardown_method(self):
        if os.path.isfile(self.queue.filename):
            os.remove(self.queue.filename)

    def test_simple(self):
        random = str(uuid.uuid4()).replace('-', '')
        filename = '{}_{}.queue'.format(self.__class__.__name__, random)

        with pytest.raises(TypeError):
            q = PersistentQueue()

        q = PersistentQueue(filename)
        assert q.maxsize == 0

        q = PersistentQueue(filename, maxsize=-42)
        assert q.maxsize == 0

        os.remove(filename)

    def test_qsize(self):
        assert len(self.queue) == 0
        assert self.queue.qsize() == 0

        assert self.queue.empty() is True
        assert self.queue.full() is False

        self.queue.put(1)
        assert len(self.queue) == 1
        assert self.queue.qsize() == 1

        self.queue.put(2)
        assert len(self.queue) == 2
        assert self.queue.qsize() == 2

        for i in range(100 + 1):
            self.queue.put(i)

        assert len(self.queue) == 103
        assert self.queue.qsize() == 103

        assert self.queue.empty() is False
        assert self.queue.full() is False

    def test_put(self):
        self.queue.put(5)
        assert self.queue.peek() == 5

        self.queue.put_nowait(10)
        assert self.queue.get() == 5
        assert self.queue.get() == 10

        self.queue.put(None)
        assert self.queue.get() is None

        data = {b'a': 1, b'b': 2, b'c': [1, 2, 3]}
        self.queue.put(data)
        assert self.queue.get() == data

        self.queue.put([])
        assert self.queue.get() == []

        self.queue.maxsize = 1
        self.queue.put(0)

        # Test that Full exception gets raised after something has been put in queue
        with pytest.raises(queue.Full):
            self.queue.put(b'full', block=True, timeout=1)

        with pytest.raises(queue.Full):
            self.queue.put(b'full', block=False)

    def test_get(self):
        self.queue.put(b'a')
        self.queue.put(b'b')
        assert len(self.queue) == 2

        assert self.queue.get() == b'a'
        assert len(self.queue) == 1

        assert self.queue.get() == b'b'
        assert len(self.queue) == 0

        self.queue.put(b'a')
        self.queue.put(b'b')
        self.queue.put(b'c')
        self.queue.put(b'd')
        assert len(self.queue) == 4

        assert [self.queue.get(), self.queue.get(), self.queue.get()] == [b'a', b'b', b'c']
        assert len(self.queue) == 1

        assert self.queue.get() == b'd'

        # Test that Empty exception gets raised after something has been removed in queue
        with pytest.raises(queue.Empty):
            self.queue.get(block=True, timeout=1)

        with pytest.raises(queue.Empty):
            self.queue.get(block=False)

        assert len(self.queue) == 0

    def test_get_blocking(self):
        done = [False]

        def func():
            time.sleep(1)
            done[0] = True
            self.queue.put(5)

        t = threading.Thread(target=func)
        t.start()

        assert done[0] is False
        data = self.queue.get()
        assert done[0] is True
        assert data == 5
        assert len(self.queue) == 0

        with pytest.raises(queue.Empty):
            self.queue.get(timeout=1)

    def test_get_non_blocking_no_values(self):
        with pytest.raises(queue.Empty):
            self.queue.get(block=False)

        with pytest.raises(queue.Empty):
            self.queue.get_nowait()

    def test_peek(self):
        self.queue.put(1)
        self.queue.put(2)
        self.queue.put(b'test')

        assert self.queue.peek() == 1
        assert self.queue.get() == 1

        assert self.queue.peek() == 2
        assert self.queue.get() == 2

        assert self.queue.peek() == b'test'
        assert self.queue.get() == b'test'

        self.queue.clear()

        self.queue.put(1)
        assert len(self.queue) == 1
        assert self.queue.peek() == 1

    def test_peek_blocking(self):
        done = [False]

        def func():
            time.sleep(1)
            done[0] = True
            self.queue.put(5)

        t = threading.Thread(target=func)
        t.start()

        assert done[0] is False
        data = self.queue.peek(block=True)
        assert done[0] is True
        assert data == 5
        assert len(self.queue) == 1

    def test_get_blocking_list(self):
        done_pushing = [False]
        done_peeking = [False]

        def func():
            for i in range(5):
                time.sleep(.1)
                self.queue.put(i)
                assert done_peeking[0] is False
            done_pushing[0] = True

        t = threading.Thread(target=func)
        t.start()

        data = [self.queue.get(block=True) for i in range(5)]
        done_peeking[0] = True
        assert done_pushing[0] is True
        assert data == [0, 1, 2, 3, 4]
        assert len(self.queue) == 0

    def test_peek_no_values(self):
        with pytest.raises(queue.Empty):
            assert self.queue.peek()

    def test_clear(self):
        self.queue.put(5)
        self.queue.put(50)

        assert len(self.queue) == 2
        assert self.queue.peek() == 5

        self.queue.clear()

        assert len(self.queue) == 0
        with pytest.raises(queue.Empty):
            assert self.queue.peek()

    def test_copy(self):
        new_queue_name = 'another_queue'
        self.queue.put(5)
        self.queue.put(4)
        self.queue.put(3)
        self.queue.put(2)
        self.queue.put(1)
        assert len(self.queue) == 5
        assert self.queue.get() == 5

        new_queue = self.queue.copy(new_queue_name)

        assert len(self.queue) == len(new_queue)
        assert self.queue.get() == new_queue.get()
        assert self.queue.get() == new_queue.get()
        assert self.queue.get() == new_queue.get()
        assert self.queue.get() == new_queue.get()

        os.remove(new_queue_name)

    def test_delete(self):
        self.queue.put(2)
        self.queue.put(3)
        self.queue.put(7)
        self.queue.put(11)
        assert len(self.queue) == 4

        self.queue.delete()
        assert len(self.queue) == 3

        self.queue.delete()
        assert len(self.queue) == 2

        assert self.queue.peek() == 7
        assert self.queue.get() == 7
        assert self.queue.get() == 11

        self.queue.put(2)
        self.queue.delete()
        self.queue.delete()
        self.queue.delete()
        self.queue.delete()
        assert len(self.queue) == 0

        self.queue.put(2)
        self.queue.put(1)
        self.queue.delete()
        assert len(self.queue) == 1

    def test_delete_no_values(self):
        assert len(self.queue) == 0
        self.queue.delete()
        assert len(self.queue) == 0

    def test_big_file_1(self):
        data = {b'a': list(range(500))}

        for i in range(1000):
            self.queue.put(data)

        assert len(self.queue) == 1000

        for i in range(995):
            assert self.queue.get() == data
            self.queue.flush()

        assert len(self.queue) == 5

    def test_big_file_2(self):
        data = {b'a': list(range(500))}

        for i in range(1000):
            self.queue.put(data)

        for i in range(995):
            assert self.queue.get() == data
        self.queue.flush()
        assert len(self.queue) == 5

        import time
        time.sleep(1)

    def test_usage(self):
        self.queue.put(1)
        self.queue.put(2)
        self.queue.put(3)
        self.queue.put([b'a', b'b', b'c'])

        assert self.queue.peek() == 1
        assert len(self.queue) == 4

        self.queue.put(b'foobar')

        assert self.queue.get() == 1
        assert len(self.queue) == 4

        for item in [2, 3, [b'a', b'b', b'c'], b'foobar']:
            assert self.queue.get() == item

    def test_threads(self):
        def random_stuff():
            for i in range(100):
                random_number = random.randint(0, 1000)

                if random_number % 3 == 0:
                    try:
                        self.queue.peek(block=False)
                    except queue.Empty:
                        pass
                elif random_number % 2 == 0:
                    try:
                        for _ in range(random_number % 5):
                            self.queue.get(block=False)
                    except queue.Empty:
                        pass
                else:
                    for i in range(random_number % 10):
                        self.queue.put({'test': [1, 2, 3], 'foo': 'bar', '1': random_number})

        threads = [threading.Thread(target=random_stuff) for _ in range(10)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # Remove everything that is left so we make sure it is serializable
        for _ in range(len(self.queue)):
            self.queue.get()

    def test_join_on_task_done(self):
        def worker():
            while True:
                try:
                    self.queue.get(block=False)
                    self.queue.task_done()
                except queue.Empty:
                    with pytest.raises(ValueError):
                        # called too many times
                        self.queue.task_done()
                    return

        self.queue.put(list(range(10)))

        t = threading.Thread(target=worker)
        t.start()

        self.queue.join()
        assert self.queue.empty() is True


class TestPersistentQueueWithDill(TestPersistentQueue):
    def setup_method(self):
        import dill

        random = str(uuid.uuid4()).replace('-', '')
        filename = '{}_{}'.format(self.__class__.__name__, random)
        self.queue = PersistentQueue(filename,
                                     loads=dill.loads,
                                     dumps=dill.dumps)


class TestPersistentQueueWithMsgpack(TestPersistentQueue):
    def setup_method(self):
        import msgpack

        random = str(uuid.uuid4()).replace('-', '')
        filename = '{}_{}'.format(self.__class__.__name__, random)
        self.queue = PersistentQueue(filename,
                                     loads=msgpack.unpackb,
                                     dumps=msgpack.packb)
