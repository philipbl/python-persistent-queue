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
        assert self.queue.peek(items=1) == 5

        self.queue.put_nowait(5)
        assert self.queue.get() == 5

        self.queue.put([10, 15, 20])
        assert self.queue.peek(items=4) == [5, 10, 15, 20]

        data = {"a": 1, "b": 2, "c": [1, 2, 3]}
        self.queue.put(data)
        assert self.queue.peek(items=5) == [5, 10, 15, 20, data]

        self.queue.put([])
        assert self.queue.peek(items=5) == [5, 10, 15, 20, data]

        self.queue.maxsize = 4
        with pytest.raises(queue.Full):
            self.queue.put('full', timeout=1)

        with pytest.raises(queue.Full):
            self.queue.put('full', block=False)

    def test_get(self):
        self.queue.put('a')
        self.queue.put('b')
        assert len(self.queue) == 2

        assert self.queue.get() == 'a'
        assert len(self.queue) == 1

        assert self.queue.get(items=1) == 'b'
        assert len(self.queue) == 0

        self.queue.put('a')
        self.queue.put('b')
        self.queue.put('c')
        self.queue.put('d')
        assert len(self.queue) == 4

        assert self.queue.get(items=3) == ['a', 'b', 'c']
        assert len(self.queue) == 1

        with pytest.raises(queue.Empty):
            assert self.queue.get(block=False, items=100) == ['d']
        assert len(self.queue) == 1

        self.queue.put('d')
        assert self.queue.get(items=0) == []
        assert len(self.queue) == 2

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
            assert self.queue.get(block=False, items=5) == []

        with pytest.raises(queue.Empty):
            self.queue.get(block=False)

        with pytest.raises(queue.Empty):
            self.queue.get_nowait()

    def test_peek(self):
        self.queue.put(1)
        self.queue.put(2)
        self.queue.put("test")

        assert self.queue.peek() == 1
        assert self.queue.peek(items=1) == 1
        assert self.queue.peek(items=2) == [1, 2]
        assert self.queue.peek(items=3) == [1, 2, "test"]

        assert self.queue.peek(items=100) == [1, 2, "test"]

        self.queue.clear()

        self.queue.put(1)
        assert len(self.queue) == 1
        assert self.queue.peek() == 1
        assert self.queue.peek(items=1) == 1
        assert self.queue.peek(items=2) == [1]

        assert self.queue.peek(items=0) == []

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

    def test_peek_blocking_list(self):
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

        data = self.queue.peek(items=5, block=True)
        done_peeking[0] = True
        assert done_pushing[0] is True
        assert data == [0, 1, 2, 3, 4]
        assert len(self.queue) == 5

    def test_peek_no_values(self):
        assert self.queue.peek(items=5) == []
        assert self.queue.peek() is None

    def test_clear(self):
        self.queue.put(5)
        self.queue.put(50)

        assert self.queue.peek(items=2) == [5, 50]
        assert len(self.queue) == 2
        self.queue.clear()
        assert len(self.queue) == 0

    def test_copy(self):
        new_queue_name = 'another_queue'
        self.queue.put([5, 4, 3, 2, 1])
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

        self.queue.delete(2)
        assert len(self.queue) == 2
        assert self.queue.peek(items=2) == [7, 11]
        assert self.queue.get(items=2) == [7, 11]

        self.queue.put(2)
        self.queue.delete(1000)
        assert len(self.queue) == 0

        self.queue.put(2)
        self.queue.delete(0)
        assert len(self.queue) == 1

    def test_delete_no_values(self):
        self.queue.delete()
        self.queue.delete(100)

    def test_big_file_1(self):
        data = {"a": list(range(500))}

        for i in range(1000):
            self.queue.put(data)

        assert len(self.queue) == 1000

        for i in range(995):
            assert self.queue.get() == data
            self.queue.flush()

        assert len(self.queue) == 5

    def test_big_file_2(self):
        data = {"a": list(range(500))}

        for i in range(1000):
            self.queue.put(data)

        assert self.queue.get(items=995) == [data for i in range(995)]
        self.queue.flush()
        assert len(self.queue) == 5

        import time
        time.sleep(1)

    def test_usage(self):
        self.queue.put(1)
        self.queue.put(2)
        self.queue.put(3)
        self.queue.put(['a', 'b', 'c'])

        assert self.queue.peek() == 1
        assert self.queue.peek(items=4) == [1, 2, 3, 'a']
        assert len(self.queue) == 6

        self.queue.put('foobar')

        assert self.queue.get() == 1
        assert len(self.queue) == 6
        assert self.queue.get(items=6) == [2, 3, 'a', 'b', 'c', 'foobar']

    def test_threads(self):
        def random_stuff():
            for i in range(100):
                random_number = random.randint(0, 1000)

                if random_number % 3 == 0:
                    try:
                        self.queue.peek(block=False, items=(random_number % 5))
                    except queue.Empty:
                        pass
                elif random_number % 2 == 0:
                    try:
                        self.queue.get(block=False, items=(random_number % 5))
                    except queue.Empty:
                        pass
                else:
                    for i in range(random_number % 10):
                        self.queue.put({"test": [1, 2, 3], "foo": "bar", "1": random_number})

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


class TestPersistentQueueWithBson:
    def setup_method(self):
        import bson

        random = str(uuid.uuid4()).replace('-', '')
        filename = '{}_{}'.format(self.__class__.__name__, random)
        self.queue = PersistentQueue(filename,
                                     loads=bson.loads,
                                     dumps=bson.dumps)

    def teardown_method(self):
        if os.path.isfile(self.queue.filename):
            os.remove(self.queue.filename)

    def test_big_file_1(self):
        data = {"a": list(range(500))}

        for i in range(1000):
            self.queue.put(data)

        assert len(self.queue) == 1000

        for i in range(995):
            assert self.queue.get() == data
            self.queue.flush()

        assert len(self.queue) == 5

    def test_big_file_2(self):
        data = {"a": list(range(500))}

        for i in range(1000):
            self.queue.put(data)

        assert self.queue.get(items=995) == [data for i in range(995)]
        self.queue.flush()
        assert len(self.queue) == 5

        import time
        time.sleep(1)
