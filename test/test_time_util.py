from unittest import TestCase
from util import time_util as tu


class TestTimeUtil(TestCase):
    def setUp(self) -> None:
        pass

    def test_ts10_to_date_str(self):
        ts = 1652840528
        result = tu.ts10_to_date_str(ts)
        self.assertEqual("2022-05-18 10:22:08", result)

        result = tu.ts10_to_date_str(ts, format_string="%Y%m%d%H%M%S")
        self.assertEqual("20220518102208", result)

    def test_ts13_to_date_str(self):
        ts = 1652840528123
        result = tu.ts13_to_date_str(ts)
        self.assertEqual("2022-05-18 10:22:08", result)

        result = tu.ts13_to_date_str(ts, format_string="%Y%m%d%H%M%S")
        self.assertEqual("20220518102208", result)
