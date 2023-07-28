"""
这是一个MySQL的工具类
提供操作MySQL的相关功能
提供的功能有:
-创建MySQL的连接
-关闭连接
-执行SQL查询的功能,并返回查询结果
-执行一条单独的无返回值的SQL语句(CREATE,UPDATE)
-创建表
-查看表是否存在
等
"""
import pymysql
from config import project_config as conf
from util.logging_util import init_logger

logger = init_logger()


class MYSQLUtil:
    def __init__(self,
                 host=conf.metadata_host,
                 port=conf.metadata_port,
                 user=conf.metadata_user,
                 password=conf.metadata_password,
                 charset=conf.mysql_charset,
                 autocommit=False):
        self.conn = pymysql.Connect(
            host=host,
            port=port,
            user=user,
            password=password,
            charset=charset,
            autocommit=False  # 自动提交
        )
        logger.info(f'构建完成到{host}:{port}的数据库连接....')

    def close_conn(self):
        """
        关闭数据连接
        :return:
        """
        if self.conn:  # 如果连接还正常
            self.conn.close()

    def query(self, sql):
        """
        执行指定的SQL语句查询,并返回查询结果
        :param sql: 需要执行的SQL语句
        :return: 返回SQL执行后的结果
        """
        # 获取可以执行sql的游标
        cursor = self.conn.cursor()
        # 执行传递进来的sql语句
        cursor.execute(sql)
        # 通过游标获取执行结果
        result = cursor.fetchall()
        # 游标关闭
        cursor.close()
        logger.info(f'执行查询的SQL语句完成,查询结果有{len(result)}条,执行的查询SQL是:{sql}')
        return result

    def select_db(self, db):
        """
        选择数据库,就是SQL中的use功能
        :param db:
        :return:
        """
        self.conn.select_db(db)

    def execute(self, sql):
        """
        直接执行一条SQL语句,不理会返回值
        :param sql:
        :return:
        """
        # 拿到游标
        cursor = self.conn.cursor()
        # 执行sql
        cursor.execute(sql)

        logger.debug(f'执行了一条SQL:{sql}')

        if not self.conn.get_autocommit():
            # 表示自动提交为False
            self.conn.commit()
        # 关闭游标
        cursor.close()

    def execute_without_autocommit(self, sql):
        """
        直接执行一条SQL语句,不理会返回值
        不会判断自动提交,只执行不会commit
        :param sql:
        :return:
        """
        # 拿到游标
        cursor = self.conn.cursor()
        # 执行SQL
        cursor.execute(sql)  # 这条SQL能否执行，取决于自动提交参数，是True就能执行，是False就暂缓
        logger.debug(f'执行了一条SQL:{sql}')

    def check_table_exists(self, db_name, table_name):
        """
        检查给定的数据库下,给定的表,是否存在
        :param db_name: 被检查的数据库
        :param table_name: 被检查的表名称
        :return: True存在,False不存在
        """
        # 切换数据库
        self.conn.select_db(db_name)
        # 查询
        # SQL查询结果是 元组嵌套元组
        result = self.query("SHOW TABLES")

        return (table_name,) in result

    def check_table_exists_and_create(self, db_name, table_name, create_cols):
        """
        检查表是否存在,如果不存在,就创建它
        :param db_name: 数据库名字
        :param table_name: 被创建的表名称
        :param create_cols: 建表语句的列信息
        :return:
        """
        # 先判断表是否存在
        if not self.check_table_exists(db_name, table_name):
            # 准备建表的SQL语句
            create_sql = f"CREATE TABLE {table_name}({create_cols})"
            # 执行建表语句
            self.conn.select_db(db_name)
            self.execute(create_sql)

            logger.info(f'在数据库:{db_name}中创建了表:{table_name}完成.建表语句是{create_sql}')
        else:
            logger.info(f'数据库:{db_name}中,表{table_name}已经存在,创建表的操作跳过.')


def get_processed_files(db_util,
                        db_name=conf.metadata_db_name,
                        table_name=conf.metadata_file_monitor_table_name,
                        create_cols=conf.metadata_file_monitor_table_create_cols):
    """
    查询已经被处理过的文件列表
    通过被查询的表,如果不存在,那么会先创建它
    :param db_util:
    :param db_name: 数据库名称
    :param table_name: 表名称
    :param create_cols: 建表语句
    :return: 返回查询到的列数据
    """
    # 已经处理过的数据的信息记录,我们存入到数据库:metadata
    # 存入到表:file_monitor中
    db_util.select_db(db_name)
    db_util.check_table_exists_and_create(
        db_name,  # 数据库名
        table_name,  # 表名
        create_cols  # 建表语句列信息
    )
    # current_timestamp 表示当前时间戳,如果设置为列的默认值,如果你插入数据不插入这个列的话,会自动添加当前时间
    result = db_util.query(f"select file_name from {table_name}")

    # 将SQL查询结果 转换为list返回
    processed_files = []
    for r in result:
        processed_files.append(r[0])

    return processed_files
