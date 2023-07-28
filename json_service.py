"""
采集JSON数据(订单数据)到
MySQL和CSV的功能代码逻辑
"""

from util.logging_util import init_logger
from util import file_util as fu
from config import project_config as conf
from util.mysql_util import MYSQLUtil, get_processed_files
from model.retail_ordrs_model import OrdersDetailModel, OrdersModel, SingleProductSoldModel

# 首先,获取到logger对象,用于后续输出日志用
logger = init_logger()
logger.info("读取JSON数据处理,程序开始执行了.....")

# 判断,JSON数据所在的文件夹下面有哪些文件可以供我们读取
files = fu.get_dir_files_list(conf.json_data_root_path)
logger.info(f"判断json的文件夹,发现有如下文件:{files}")

# 获取元数据连接的db_util对象
metadata_db_util = MYSQLUtil()
# 获取目标数据库连接的db_util对象
target_db_util = MYSQLUtil(conf.target_host, conf.target_port, conf.target_user, conf.target_password)

# 获取哪些文件是已经处理过的
processed_files = get_processed_files(metadata_db_util)
logger.info(f'查询MySQL,找到有如下文件已经被处理过了:{processed_files}')

# 对比files和processed_files,找到没有被处理的文件供我们使用
need_to_processed_files = fu.get_new_by_compare_list(processed_files, files)
logger.info(f'经过对比MySQL元数据库,找到如下文件供我们处理:'
            f'{need_to_processed_files}')

# 开始处理文件
# 依次处理文件
global_count = 0
processed_files_record_dict = {}
for file in need_to_processed_files:
    # 记录此文件被处理了多少条的计数器
    file_processed_lines_count = 0
    # 存储所有的订单模型对象
    order_model_list = []
    # 存储所有的订单详情模型对象
    order_detail_model_list = []

    # 通过open读取文件
    for line in open(file, "r", encoding="utf-8"):
        global_count += 1
        file_processed_lines_count += 1
        # line就是每一行数据,先将里面的 回车符删除
        line = line.replace("\n", "")
        order_model = OrdersModel(line)
        order_detail_model = OrdersDetailModel(line)
        order_model_list.append(order_model)
        order_detail_model_list.append(order_detail_model)

    # 过滤数据
    # 数据中,有一个字段,receivable表示订单卖了多少钱
    # 数据中有许多的测试数据,receivable的金额非常大,我们做一个简单的判断,大于10000的这个数据我们就不要了
    reserved_models = []
    for model in order_model_list:
        if model.receivable <= 10000:
            reserved_models.append(model)

    # 写出csv和MySQL
    # 用来写出订单模型的文件对象
    order_csv_write_f = open(
        conf.retail_output_csv_root_path +
        conf.retail_order_output_csv_file_name, "a", encoding="utf-8"
    )
    # 用来写出订单详情模型的文件对象
    order_detail_csv_write_f = open(
        conf.retail_output_csv_root_path +
        conf.retail_order_detail_output_csv_file_name, "a", encoding="utf-8"
    )
    # 先处理订单模型
    csv_count = 0
    for model in reserved_models:
        csv_line = model.to_csv()
        # 写出一行csv数据
        order_csv_write_f.write(csv_line)
        # 再写出一个换行符
        order_csv_write_f.write("\n")
        csv_count += 1
        if csv_count % 1000 == 0:
            order_csv_write_f.flush()
    order_csv_write_f.close()

    # 接着处理订单详情模型
    csv_count = 0
    for model in order_detail_model_list:
        # 每一个model就是一个OrdersDetailModel对象
        for single_product_model in model.products_detail:
            csv_line = single_product_model.to_csv()
            # 写出一行csv数据
            order_detail_csv_write_f.write(csv_line)
            # 再写出一个换行符
            order_detail_csv_write_f.write("\n")
            csv_count += 1
            if csv_count % 1000 == 0:
                order_detail_csv_write_f.flush()
    order_detail_csv_write_f.close()

    # 将数据写出到MySQL中
    # 先判断被写入的MySQL的表是否存在,如果不存在,先创建它们
    # 先判断订单表
    target_db_util.check_table_exists_and_create(
        conf.target_db_name,
        conf.target_orders_table_name,
        conf.target_orders_table_create_cols
    )

    # 在判断订单详情表
    target_db_util.check_table_exists_and_create(
        conf.target_db_name,
        conf.target_orders_detail_table_name,
        conf.target_orders_detail_table_create_cols
    )

    # 先将订单数据,写入订单表
    for model in reserved_models:
        # 每一个model就是一个OrdersModel的订单模型的对象
        insert_sql = model.generate_insert_sql()
        # 选择被操作数据库
        target_db_util.select_db(conf.target_db_name)
        # 选择target_db_util,使用execute方法插入数据
        target_db_util.execute_without_autocommit(insert_sql)

    # 写入订单详情数据
    for model in order_detail_model_list:
        # 每一个model就是一个OrderDetailModel
        insert_sql = model.generate_insert_sql()
        # 选择被操作数据库
        target_db_util.select_db(conf.target_db_name)
        # 选择target_db_util,使用execute方法插入数据
        target_db_util.execute_without_autocommit(insert_sql)

    processed_files_record_dict[file] = file_processed_lines_count

# 一次批量将缓冲区内的暂存insert sql语句全部一次性提交到MySQL
target_db_util.conn.commit()

logger.info(f"完成了csv备份文件的写出,写出到了:{conf.retail_output_csv_root_path}")
logger.info(f"完成了向MySQL数据库中插入数据的操作."
            f"共处理了:{global_count}条数据")

# 将已经处理完成的文件,记录到MySQL中
for file_name in processed_files_record_dict.keys():
    # 文件被处理的行
    file_processed_lines = processed_files_record_dict[file_name]
    # 文件的名,就是file_name本身

    # 组装insert into
    insert_sql = f"insert into {conf.metadata_file_monitor_table_name}(file_name,process_lines)" \
                 f"values ('{file_name}',{file_processed_lines})"
    metadata_db_util.execute(insert_sql)

# 最后将所有的数据库链接关闭
metadata_db_util.close_conn()
target_db_util.close_conn()
logger.info("读取JSON数据向MySQL插入以及写出csv备份,程序执行完成......")
