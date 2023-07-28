"""
测试 日志的工具方法
"""
from unittest import TestCase
import logging
from util import logging_util


class TestLoggingUtil(TestCase):
    def setUp(self) -> None:
        pass

    def test_get_logger(self):
        logger = logging_util.init_logger()
        result = isinstance(logger, logging.RootLogger)
        self.assertEqual(True, result)
