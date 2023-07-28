"""
采集MySQL中的条码库(商品信息)数据
采集到目标的MySQL中
"""
import sys
from util.logging_util import init_logger
from util.mysql_util import MYSQLUtil
from config import project_config as conf
from model.barcode_model import BarcodeModel

# 构建日志对象
logger = init_logger()
logger.info("采集MySQL数据的程序启动.........")

# TODO:步骤1:构建数据库util对象

# 构建MySQL的数据util工具
# 这个需求设计到:元数据,数据源,目的地 三个数据库,我们构建3个db_util工具
# 构建元数据的util对象
metadata_db_util = MYSQLUtil()
# 构建数据源数据库的util对象
source_db_util = MYSQLUtil(
    host=conf.source_host,
    user=conf.source_user,
    password=conf.source_password,
    port=conf.source_port
)
# 构建目的地数据库的util对象
target_db_util = MYSQLUtil(
    host=conf.target_host,
    user=conf.target_user,
    password=conf.target_password,
    port=conf.target_port
)

# TODO:步骤2:从数据源中读取数据
# 先判断:供我们读取数据的表是否存在?如果不存在?程序退出
if not source_db_util.check_table_exists(conf.source_db_name, conf.source_barcode_data_table_name):
    # 进入if 表示表不存在
    logger.error(f"数据源库:{conf.source_db_name}中不存在数据源表:{conf.source_barcode_data_table_name},"
                 f"无法采集,程序退出,请开展社交找人要去..........")
    sys.exit(1)  # sys.exit()表示Python程序停止运行,传入0表示是正常的停止,传入非0的数字表示程序是异常停止

# 接着判断,目的地的MySQL库中,是否有我们要写入数据的表?如果没有? 就新建
target_db_util.check_table_exists_and_create(
    conf.target_db_name,
    conf.target_barcode_table_name,
    conf.target_barcode_table_create_cols
)

# 开始查询的流程了
# 注意:我们每一次查询,都要去判断一下update_at,来决定本次查询从哪个时间开始
# 上一次查询的最大时间,会记录到MySQL中,我们在查询数据之前,需要先从MySQL中查询出来
# 上一次查询的时间
# 选择元数据库
metadata_db_util.select_db(conf.metadata_db_name)

# 从元数据表中查询上一次的记录,先判断,这个元数据表是否存在,不存在?要创建
# 定义一个变量,记录上一次查询的时间
last_update_time = None
if not metadata_db_util.check_table_exists(conf.metadata_db_name, conf.metadata_barcode_table_name):
    # 进入if,表示表不存在,需要创建表
    metadata_db_util.check_table_exists_and_create(
        conf.metadata_db_name,
        conf.metadata_barcode_table_name,
        conf.metadata_barcode_table_create_cols
    )
else:
    # 进入else表示表存在,需要从MySQL中查询出上一次查询的时间
    query_sql = f"select time_record from {conf.metadata_barcode_table_name} order by time_record desc limit 1"
    result = metadata_db_util.query(query_sql)
    # 表存在 不代表里面有数据
    if len(result) != 0:
        # 代表有表,并且查询到数据了
        last_update_time = str(result[0][0])

# 准备SQL查询的语句
if last_update_time:
    # 表示last_update_time不是None
    sql = f"select * from {conf.source_barcode_data_table_name} where updateAt >= '{last_update_time}'" \
          f"order by updateAt"
else:
    # 表示last_update_time 是None
    sql = f"select * from {conf.source_barcode_data_table_name} order by updateAt"
# 开始执行SQL查询
# 选择数据库
source_db_util.select_db(conf.source_db_name)
# 执行SQL查询
result = source_db_util.query(sql)
# pymysql的查询结果是:((结果的某行), (结果的某行), (结果的某行), (结果的某行), ......)
# (结果的某行)是:元组类型,里面存储的是查询结果的列,比如(1, '张三', 11) .......
# TODO:步骤3:开始准备模型了构建insert into插入语句

barcode_models = []
for single_line_result in result:
    # single_line_result是元组,存储了一条结果的列
    # 查询select * 查询,结果中的第一个列 第二个列 第N个列是谁,和数据库的表结构顺序一致
    code = single_line_result[0]
    name = single_line_result[1]
    spec = single_line_result[2]
    trademark = single_line_result[3]
    addr = single_line_result[4]
    units = single_line_result[5]
    factory_name = single_line_result[6]
    trade_price = single_line_result[7]
    retail_price = single_line_result[8]
    update_at = str(single_line_result[9])  # single_line_result[9]是读取的updateAt时间,类型是datetime,转换成字符串
    wholeunit = single_line_result[10]
    wholenum = single_line_result[11]
    img = single_line_result[12]
    src = single_line_result[13]

    # 构建BarcodeModel
    model = BarcodeModel(
        code=code,
        name=name,
        spec=spec,
        trademark=trademark,
        addr=addr,
        units=units,
        factory_name=factory_name,
        trade_price=trade_price,
        retail_price=retail_price,
        update_at=update_at,
        wholeunit=wholeunit,
        wholenum=wholenum,
        img=img,
        src=src,
    )
    barcode_models.append(model)

# 至此,我们有了barcode_models list,里面存放了一堆barcode_models对象

# TODO:步骤4:开始写入
# 切换数据库
target_db_util.select_db(conf.target_db_name)
max_last_update_time = "2001-01-01 00:00:00"
# for循环 挨个执行SQL插入
count = 0
for model in barcode_models:
    # 取出'''本'''条数据的处理时间,current:当前的
    current_data_time = model.update_at
    # 判断当前这个时间是否大于记录的max_last_update_time
    if current_data_time > max_last_update_time:
        max_last_update_time = current_data_time

    # 生成插入SQL语句
    insert_sql = model.generate_insert_sql()
    # 执行插入,使用execute_without_autocommit配合autocommit为False应用批量提交提高性能
    target_db_util.execute_without_autocommit(insert_sql)

    count += 1
    if count % 1000 == 0:
        # 每隔1000条提交一次
        target_db_util.conn.commit()
        logger.info(f"从数据源:{conf.source_db_name}库,读取表:"
                    f"{conf.source_barcode_data_table_name},"
                    f"当前写入目标表:{conf.target_barcode_table_name}数据有:{count}行")
# 一次性批量提交
target_db_util.conn.commit()
logger.info(f"从数据源:{conf.source_db_name}库,读取表:"
            f"{conf.source_barcode_data_table_name},"
            f"当前写入目标表:{conf.target_barcode_table_name}完成,最终写入:{count}行")
# 写入csv
barcode_csv_write_f = open(
    conf.barcode_output_csv_root_path + conf.barcode_order_output_csv_file_name,
    "a",
    encoding="utf-8"
)
count = 0
for model in barcode_models:
    csv_line = model.to_csv()
    barcode_csv_write_f.write(csv_line)
    barcode_csv_write_f.write("\n")
    count += 1

    if count % 1000 == 0:
        # 每隔1000条,清空缓冲区,将内容写入文件
        barcode_csv_write_f.flush()
        logger.info(f"从数据源:{conf.source_db_name}库,读取表:"
                    f"{conf.source_barcode_data_table_name},"
                    f"写出csv到:{barcode_csv_write_f.name}完成,最终写入:{count}行")
barcode_csv_write_f.close()
logger.info(f"从数据源:{conf.source_db_name}库,读取表:"
            f"{conf.source_barcode_data_table_name},"
            f"写出csv到:{barcode_csv_write_f.name}完成,最终写入:{count}行")

# TODO:步骤5:记录MySQL元数据
metadata_db_util.select_db(conf.metadata_db_name)
# 准备SQL
sql = f"insert into {conf.metadata_barcode_table_name}(" \
      f"time_record,gather_line_count) values (" \
      f"'{max_last_update_time}'," \
      f"{count}" \
      f")"
# 执行插入
metadata_db_util.execute(sql)
# 关闭全部数据库连接
metadata_db_util.close_conn()
source_db_util.close_conn()
target_db_util.close_conn()

logger.info("读取MySQL数据,写入目标MySQL和csv程序执行完成.......")
