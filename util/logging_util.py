"""
这个python文件的功能: 构建日志输出的模块
"""
import logging
from config import project_config as conf


# 封装一个class,提供基本的logging对象(没啥属性,只有级别默认为INFO)
class Logging:
    def __init__(self, level=20):
        self.logger = logging.getLogger()
        self.logger.setLevel(level)


# 构建一个方法,我们可以通过这个方法返回所需的logger
def init_logger():
    # 初始化Logging类,得到logger对象
    logger = Logging().logger

    if logger.handlers:
        return logger

    # 对logger对象设置属性
    file_handler = logging.FileHandler(
        filename=conf.log_root_path + conf.log_name,
        mode="a",
        encoding="utf-8"
    )
    # 设置一个format输出格式
    fmt = logging.Formatter(
        "%(asctime)s - [%(levelname)s] - %(filename)s[%(lineno)d]: %(message)s"
    )
    # 将格式设置到文件的handler中
    file_handler.setFormatter(fmt)
    # 将文件输出的Handler设置给logger对象
    logger.addHandler(file_handler)

    return logger


# init_logger().info("这是个info日志")
# init_logger().warning("这是个warn日志")
