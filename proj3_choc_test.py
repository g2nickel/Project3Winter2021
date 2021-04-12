import unittest
from proj3_choc import *

# proj3_choc_test.py
# You must NOT change this file. You can comment stuff out while debugging but
# don't forget to restore it to its original form!

class TestBarSearch(unittest.TestCase):

    def test_bar_search(self):
        results = process_command('bars ratings top 1')
        self.assertEqual(results[0][0], 'Chuao')

        results = process_command('bars cocoa bottom 10')
        self.assertEqual(results[0][0], 'Guadeloupe')

        results = process_command('bars sell country=CA ratings top 5')
        self.assertEqual(results[0][3], 4.0)

        results = process_command('bars source region=Africa ratings top 5')
        self.assertEqual(results[0][3], 4.0)


class TestCompanySearch(unittest.TestCase):

    def test_company_search(self):
        results = process_command('companies region=Europe ratings top 5')
        self.assertEqual(results[1][0], 'Idilio (Felchlin)')

        results = process_command('companies country=US number_of_bars top 5')
        self.assertTrue(results[0][0] == 'Fresco' and results[0][2] == 26)

        results = process_command('companies cocoa top 5')
        self.assertEqual(results[0][0], 'Videri')
        self.assertGreater(results[0][2], 0.79)

class TestCountrySearch(unittest.TestCase):

    def test_country_search(self):
        results = process_command('countries source ratings bottom 5')
        self.assertEqual(results[1][0],'Uganda')

        results = process_command('countries sell number_of_bars top 5')
        self.assertEqual(results[0][2], 764)
        self.assertEqual(results[1][0], 'France')


class TestRegionSearch(unittest.TestCase):

    def test_region_search(self):
        results = process_command('regions source number_of_bars top 5')
        self.assertEqual(results[0][0], 'Americas')
        self.assertEqual(results[3][1], 66)
        self.assertEqual(len(results), 5)

        results = process_command('regions sell ratings top 10')
        self.assertEqual(len(results), 5)
        self.assertEqual(results[0][0], 'Oceania')
        self.assertGreater(results[3][1], 3.0)

unittest.main()
