"""
需求3:主业务逻辑文件,采集后台日志
"""
from config import project_config as conf
from util import file_util as fu
from util.mysql_util import MYSQLUtil, get_processed_files
from model.backend_logs_model import BackendLogsModel

# TODO: 步骤1:读取文件夹中有哪些文件
file = fu.get_dir_files_list(conf.log_data_root_path)

# TODO: 步骤3:读取MySQL元数据库
# 构建元数据库连接db_util对象
metadata_db_util = MYSQLUtil()

# 构建目标库的db_util对象
log_db_util = MYSQLUtil(
    host=conf.log_host,
    port=conf.log_port,
    user=conf.log_user,
    password=conf.log_password,
    charset=conf.mysql_charset,
    autocommit=False
)
# 检查要写入数据的目标表是否存在,不存在就创建
log_db_util.check_table_exists_and_create(
    conf.log_data_db_name,
    conf.log_data_table_name,
    create_cols=conf.log_data_table_create_cols
)

# 检查元数据的表是否存在
# 工具方法获得MySQL中那些数据被处理了
processed_files = get_processed_files(
    db_util=metadata_db_util,
    db_name=conf.log_file_db_name,
    table_name=conf.log_file_table_name,
    create_cols=conf.log_file_table_create_cols
)

# TODO: 步骤3:对比找出那些文件可以被处理
# need需要 process处理
need_to_process_files = fu.get_new_by_compare_list(processed_files, file)

# 构建一个文件对象,用于写出csv
backend_logs_write_f = open(
    conf.log_output_csv_root_path + conf.log_order_output_csv_file_name,
    "a",
    encoding='utf-8'
)

# 全局计数器
global_count = 0
# 构建一个字段,记录每一个文件被处理的行数
processed_files_record_dict = {}
# TODO: 步骤4:读取文件每一行进行处理
for file in need_to_process_files:
    single_file_count = 0  # 针对每一个文件被处理的行数的计数器
    for line in open(file, "r", encoding="utf-8"):
        line = line.replace("\n", "")  # 处理掉每一行的\n回车符
        # TODO: 步骤5:构建模型
        model = BackendLogsModel(line)

        # TODO: 步骤6:写出MySQL(不要忘记构建目标库的db_util对象,以及检查目标表是否存在,不存在创建,参见16~30行)
        insert_sql = model.generate_insert_sql()
        log_db_util.select_db(conf.log_data_db_name)
        log_db_util.execute_without_autocommit(insert_sql)

        # TODO: 步骤7:写出csv(不要忘记工具写出csv的文件对象)
        backend_logs_write_f.write(model.to_csv())
        backend_logs_write_f.write("\n")

        global_count += 1
        single_file_count += 1
        if global_count % 1000 == 0:
            # 提交MySQL
            log_db_util.conn.commit()
            # 刷新文件写出的缓冲区
            backend_logs_write_f.flush()

    processed_files_record_dict[file] = single_file_count

log_db_util.conn.commit()
backend_logs_write_f.flush()

# TODO: 步骤8:记录元数据
for file_name in processed_files_record_dict.keys():
    count = processed_files_record_dict[file_name]
    sql = f"insert into {conf.log_file_table_name}(file_name, process_lines) values (" \
          f"'{file_name}',{count})"
    metadata_db_util.select_db(conf.log_file_db_name)
    metadata_db_util.execute(sql)

metadata_db_util.close_conn()
log_db_util.close_conn()
