import unittest

from pyfakefs import fake_filesystem_unittest

from persistent_list import PersistentList


class TestPersistentList(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def test_getitem_bad_index(self):
        lst = PersistentList()

        with self.assertRaises(TypeError):
            lst["x"]

        with self.assertRaises(TypeError):
            lst[0, 1, 2]

    def test_getitem(self):
        lst = PersistentList()
        lst.append(1001)
        lst.append(1002)
        lst.append(1003)

        self.assertEqual(lst[0], 1001)
        self.assertEqual(lst[1], 1002)
        self.assertEqual(lst[2], 1003)

    def test_count(self):
        lst = PersistentList()
        self.assertEqual(len(lst), 0)

        lst.append(1)
        self.assertEqual(len(lst), 1)

        lst.append(2)
        self.assertEqual(len(lst), 2)

        for i in range(100 + 1):
            lst.append(i)

        self.assertEqual(len(lst), 103)

    def test_clear(self):
        lst = PersistentList()
        lst.append(5)
        lst.append(50)

        self.assertEqual(lst[0], 5)
        self.assertEqual(lst[1], 50)
        self.assertEqual(len(lst), 2)

        lst.clear()

        self.assertEqual(len(lst), 0)

    def test_getitem_slice(self):
        lst = PersistentList()
        lst.append(50)
        lst.append(49)
        lst.append(48)

        self.assertEqual(lst[0:1], [50])
        self.assertEqual(lst[0:2], [50, 49])
        self.assertEqual(lst[0:3], [50, 49, 48])

        # TODO: This should work
        # self.assertEqual(lst[0:100], [50, 49, 48])

        # self.assertEqual(lst[:], [50, 49, 48])


if __name__ == '__main__':
    unittest.main()
