import unittest

from pyfakefs import fake_filesystem_unittest

from persistent_queue import PersistentQueue


class TestPersistentQueue(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def test_count(self):
        queue = PersistentQueue()
        self.assertEqual(len(queue), 0)
        self.assertEqual(queue.count(), 0)

        queue.push(1)
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue.count(), 1)

        queue.push(2)
        self.assertEqual(len(queue), 2)
        self.assertEqual(queue.count(), 2)

        for i in range(100 + 1):
            queue.push(i)

        self.assertEqual(len(queue), 103)
        self.assertEqual(queue.count(), 103)

    def test_clear(self):
        queue = PersistentQueue()
        queue.push(5)
        queue.push(50)

        self.assertEqual(queue.peek(2), [5, 50])
        self.assertEqual(len(queue), 2)
        queue.clear()
        self.assertEqual(len(queue), 0)

    def test_push(self):
        queue = PersistentQueue()
        queue.push(5)
        self.assertEqual(queue.peek(1), 5)

        queue.push([10, 15, 20])
        self.assertEqual(queue.peek(4), [5, 10, 15, 20])

        data = {"a": 1, "b": 2, "c": [1, 2, 3]}
        queue.push(data)
        self.assertEqual(queue.peek(5), [5, 10, 15, 20, data])

    def test_pop(self):
        queue = PersistentQueue()
        queue.push('a')
        queue.push('b')
        self.assertEqual(len(queue), 2)

        self.assertEqual(queue.pop(), 'a')
        self.assertEqual(len(queue), 1)

        self.assertEqual(queue.pop(1), 'b')
        self.assertEqual(len(queue), 0)

        queue.push('a')
        queue.push('b')
        queue.push('c')
        queue.push('d')
        self.assertEqual(len(queue), 4)

        self.assertEqual(queue.pop(3), ['a', 'b', 'c'])
        self.assertEqual(len(queue), 1)

        self.assertEqual(queue.pop(100), ['d'])
        self.assertEqual(len(queue), 0)

        self.assertEqual(queue.pop(100), [])
        self.assertEqual(queue.pop(), None)

    def test_peek(self):
        queue = PersistentQueue()
        queue.push(1)
        queue.push(2)
        queue.push("test")

        self.assertEqual(queue.peek(), 1)
        self.assertEqual(queue.peek(1), 1)
        self.assertEqual(queue.peek(2), [1, 2])
        self.assertEqual(queue.peek(3), [1, 2, "test"])

        self.assertEqual(queue.peek(100), [1, 2, "test"])

        queue.clear()

        queue.push(1)
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue.peek(), 1)
        self.assertEqual(queue.peek(1), 1)
        self.assertEqual(queue.peek(2), [1])




if __name__ == '__main__':
    unittest.main()
