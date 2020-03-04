import unittest
from lineparser import LineParser

class TestLineParser(unittest.TestCase):
    def test_parse(self):
        cases = [
            (r'-1,"Unknown",\N,"-","N/A",\N,\N,"Y"', ('nq', 'q', 'q', 'q', 'q', 'nq', 'nq', 'q'), ['-1', 'Unknown', None, '-', 'N/A', None, None, 'Y'], None),
            (r'123,"SN","SG"', ('q', 'q', 'q'), [], ValueError),
            (r'0,"A long string, which also contains commas","SG",\N', ('nq', 'q', 'q', 'q'), ['0', 'A long string, which also contains commas', 'SG', None], None),
            (r'abcdef,"ghijkl",\N,"12345"', ('nq', 'q', 'nq', 'q'), ['abcdef', 'ghijkl', None, '12345'], None),
        ]

        for l, fmt, want, want_ex in cases:
            lp = LineParser(fmt)
            if want_ex:
                with self.assertRaises(want_ex):
                    lp.parse(l)
            else:
                self.assertEqual(lp.parse(l), want)


if __name__ == "__main__":
    unittest.main()