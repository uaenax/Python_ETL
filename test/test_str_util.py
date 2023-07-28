from unittest import TestCase
from util import str_util as su


class TestStrUtil(TestCase):
    def setUp(self) -> None:
        pass

    def test_check_null(self):
        s = None
        result = su.check_null(s)
        self.assertTrue(result)

        s = 'None'
        result = su.check_null(s)
        self.assertTrue(result)

        s = 'NONE'
        result = su.check_null(s)
        self.assertTrue(result)

        s = 'null'
        result = su.check_null(s)
        self.assertTrue(result)

        s = ''
        result = su.check_null(s)
        self.assertTrue(result)

        s = 'undefined'
        result = su.check_null(s)
        self.assertTrue(result)

        s = '我是有意义的字符串'
        result = su.check_null(s)
        self.assertFalse(result)

    def test_check_str_null_and_transfrom_to_sql_null(self):
        name = ""
        result = su.check_str_null_and_transform_to_sql_null(name)
        self.assertEqual("NULL", result)

        name = "None"
        result = su.check_str_null_and_transform_to_sql_null(name)
        self.assertEqual("NULL", result)

        name = "null"
        result = su.check_str_null_and_transform_to_sql_null(name)
        self.assertEqual("NULL", result)

        name = "undefined"
        result = su.check_str_null_and_transform_to_sql_null(name)
        self.assertEqual("NULL", result)

        name = "张三"
        result = su.check_str_null_and_transform_to_sql_null(name)
        self.assertEqual("'张三'", result)
