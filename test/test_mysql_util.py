"""
测试MySQL工具类的一系列功能
"""
from unittest import TestCase
from util.mysql_util import MYSQLUtil, get_processed_files
from config import project_config as conf


class TestMySQLUtil(TestCase):
    def setUp(self) -> None:
        # 作用雷同于普通类中的__init__方法,同样是做初始化操作的
        self.db_util = MYSQLUtil()

    def test_query(self):
        """
        测试使用的数据库名称为:TestMySQLUtil
        测试使用的表名称为:for_unit_test
        :return:
        """
        # 测试MySQLUtil中的query方法
        # 不使用MySQL中已经存在的表,解耦合,确保单元测试的独立性
        self.db_util.execute("create database TestMySQLUtil")
        self.db_util.select_db("TestMySQLUtil")
        self.db_util.check_table_exists_and_create(
            "TestMySQLUtil",
            "for_unit_test",
            "id int primary key, name varchar(255)"
        )
        self.db_util.execute("truncate for_unit_test")
        self.db_util.execute(
            "insert into for_unit_test values (1, '潇潇'),(2, '甜甜')"
        )
        # 数据准备完毕
        result = self.db_util.query("select * from for_unit_test order by id")
        expected = ((1, '潇潇'), (2, '甜甜'))
        self.assertEqual(expected, result)

        # 清理单元测试的残留
        self.db_util.execute("drop table for_unit_test")
        self.db_util.execute("drop database TestMySQLUtil")
        self.db_util.close_conn()

    def test_execute_without_autocommit(self):
        """
        测试使用的数据库名称为:TestMySQLUtil
        测试使用的表名称为:for_unit_test
        :return:
        """
        # 设置AutoCommit为True
        self.db_util.conn.autocommit(True)
        self.db_util.execute("create database TestMySQLUtil")
        self.db_util.select_db("TestMySQLUtil")
        self.db_util.check_table_exists_and_create(
            "TestMySQLUtil",
            "for_unit_test",
            "id int primary key, name varchar(255)"
        )
        self.db_util.execute("truncate for_unit_test")
        self.db_util.execute_without_autocommit(
            "insert into for_unit_test values (1, '潇潇')"
        )
        # 数据准备完毕
        result = self.db_util.query("select * from for_unit_test order by id")
        expected = ((1, '潇潇'),)
        self.assertEqual(expected, result)

        # 清理单元测试的残留
        # self.db_util.execute("drop table for_unit_test")
        self.db_util.close_conn()

        # 设置AutoCommit为False
        new_util = MYSQLUtil()
        new_util.conn.autocommit(False)
        new_util.select_db('TestMySQLUtil')
        new_util.execute_without_autocommit(
            "insert into for_unit_test values (2, '甜甜')"
        )
        new_util.close_conn()

        new_util2 = MYSQLUtil()
        new_util2.select_db('TestMySQLUtil')
        result = new_util2.query("select * from for_unit_test order by id")
        expected = ((1, '潇潇'),)
        self.assertEqual(expected, result)

        # 清理单元测试的残留
        new_util2.execute("drop table for_unit_test")
        new_util2.execute("drop database TestMySQLUtil")
        new_util2.close_conn()

    def test_get_processed_files(self):
        """
        测试获取已经被处理过的文件列表功能的单元测试
        保证独立性,自备表和数据
        :return:
        """
        self.db_util.execute("create database TestMySQLUtil")
        self.db_util.select_db("TestMySQLUtil")
        self.db_util.check_table_exists_and_create(
            "TestMySQLUtil",
            "for_unit_test",
            conf.metadata_file_monitor_table_create_cols
        )
        self.db_util.execute("truncate for_unit_test")
        self.db_util.execute(
            """
            insert into for_unit_test values (1, 'e:/data.log', 1024, '2000-01-01 10:00:00')
            """
        )
        # 通过写的工具方法,去查询文件列表
        result = get_processed_files(self.db_util, "TestMySQLUtil", "for_unit_test")
        expected = ['e:/data.log']
        # 验证
        self.assertEqual(expected, result)
        # 清理单元测试的残留
        self.db_util.execute("drop table for_unit_test")
        self.db_util.execute("drop database TestMySQLUtil")
        self.db_util.close_conn()


