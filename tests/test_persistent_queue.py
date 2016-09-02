import os
import unittest
import uuid

from persistent_queue import PersistentQueue


class TestPersistentQueue(unittest.TestCase):
    def setUp(self):
        random = str(uuid.uuid4()).replace('-', '')
        self.filename = '{}_{}'.format(self.id(), random)
        self.queue = PersistentQueue(self.filename)

    def tearDown(self):
        os.remove(self.filename)

    def test_count(self):

        self.assertEqual(len(self.queue), 0)
        self.assertEqual(self.queue.count(), 0)

        self.queue.push(1)
        self.assertEqual(len(self.queue), 1)
        self.assertEqual(self.queue.count(), 1)

        self.queue.push(2)
        self.assertEqual(len(self.queue), 2)
        self.assertEqual(self.queue.count(), 2)

        for i in range(100 + 1):
            self.queue.push(i)

        self.assertEqual(len(self.queue), 103)
        self.assertEqual(self.queue.count(), 103)

    def test_clear(self):
        self.queue.push(5)
        self.queue.push(50)

        self.assertEqual(self.queue.peek(2), [5, 50])
        self.assertEqual(len(self.queue), 2)
        self.queue.clear()
        self.assertEqual(len(self.queue), 0)

    def test_push(self):
        self.queue.push(5)
        self.assertEqual(self.queue.peek(1), 5)

        self.queue.push([10, 15, 20])
        self.assertEqual(self.queue.peek(4), [5, 10, 15, 20])

        data = {"a": 1, "b": 2, "c": [1, 2, 3]}
        self.queue.push(data)
        self.assertEqual(self.queue.peek(5), [5, 10, 15, 20, data])

    def test_pop(self):
        self.queue.push('a')
        self.queue.push('b')
        self.assertEqual(len(self.queue), 2)

        self.assertEqual(self.queue.pop(), 'a')
        self.assertEqual(len(self.queue), 1)

        self.assertEqual(self.queue.pop(1), 'b')
        self.assertEqual(len(self.queue), 0)

        self.queue.push('a')
        self.queue.push('b')
        self.queue.push('c')
        self.queue.push('d')
        self.assertEqual(len(self.queue), 4)

        self.assertEqual(self.queue.pop(3), ['a', 'b', 'c'])
        self.assertEqual(len(self.queue), 1)

        self.assertEqual(self.queue.pop(100), ['d'])
        self.assertEqual(len(self.queue), 0)

        self.assertEqual(self.queue.pop(100), [])
        self.assertEqual(self.queue.pop(), None)

    def test_peek(self):
        self.queue.push(1)
        self.queue.push(2)
        self.queue.push("test")

        self.assertEqual(self.queue.peek(), 1)
        self.assertEqual(self.queue.peek(1), 1)
        self.assertEqual(self.queue.peek(2), [1, 2])
        self.assertEqual(self.queue.peek(3), [1, 2, "test"])

        self.assertEqual(self.queue.peek(100), [1, 2, "test"])

        self.queue.clear()

        self.queue.push(1)
        self.assertEqual(len(self.queue), 1)
        self.assertEqual(self.queue.peek(), 1)
        self.assertEqual(self.queue.peek(1), 1)
        self.assertEqual(self.queue.peek(2), [1])

    def test_big_file(self):
        data = {"a": list(range(1000))}

        for i in range(2000):
            self.queue.push(data)

        for i in range(1995):
            self.assertEqual(self.queue.pop(), data)
            self.queue.flush()

        self.assertEqual(len(self.queue), 5)

    def test_big_file_2(self):
        data = {"a": list(range(1000))}

        for i in range(2000):
            self.queue.push(data)

        self.assertEqual(self.queue.pop(1995), [data for i in range(1995)])
        self.assertEqual(len(self.queue), 5)

    def test_copy(self):
        new_queue_name = 'another_queue'
        self.queue.push([5, 4, 3, 2, 1])
        self.assertEqual(len(self.queue), 5)
        self.assertEqual(self.queue.pop(), 5)

        new_queue = self.queue.copy(new_queue_name)

        self.assertEqual(len(self.queue), len(new_queue))
        self.assertEqual(self.queue.pop(), new_queue.pop())
        self.assertEqual(self.queue.pop(), new_queue.pop())
        self.assertEqual(self.queue.pop(), new_queue.pop())
        self.assertEqual(self.queue.pop(), new_queue.pop())

        os.remove(new_queue_name)


if __name__ == '__main__':
    unittest.main()
