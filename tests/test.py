from hours import *
import unittest

class Tests(unittest.TestCase):
    def test_main (self):
        self.assertEqual(200, main())

    def test_get_ids (self):
        result = get_ids ()
        self.assertTrue(result['codes'])

if __name__ == '__main__':
    unittest.main(verbosity=2)