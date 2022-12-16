from hours import *
import unittest

class Tests(unittest.TestCase):
    """
        def test_main (self):
        self.assertEqual(200, main())
    """
    
    def test_get_ids (self):
        result = get_notion_data()
        self.assertTrue(result['codes'])

    def test_get_time(self):
        init_date = datetime(2022,12,1)
        last_date = init_date + timedelta(days=1)
        result = get_time_record(init_date=format_date(init_date),end_date=format_date(last_date),ids_clockify=[], hours_clockify=[])
        self.assertEqual(200, result)

if __name__ == '__main__':
    unittest.main(verbosity=2)