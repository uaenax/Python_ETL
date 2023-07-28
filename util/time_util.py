"""
此文件是和时间相关的工具代码集合
"""
import time


def ts10_to_date_str(ts, format_string="%Y-%m-%d %H:%M:%S"):
    """
    将10位(秒级)的时间戳,转化成给定的日期格式
    :param ts: 即将被转换的时间戳数字
    :param format_string: 转换后的日期格式,默认是: 2022-01-01 10:00:00的格式
    :return: 转化完成后的日期字符串
    """
    # 将时间戳转化成时间数组(中转格式)
    time_array = time.localtime(ts)
    # 将时间数组格式化为指定的格式
    return time.strftime(format_string, time_array)


def ts13_to_date_str(ts, format_string="%Y-%m-%d %H:%M:%S"):
    """
    将13位(毫秒级)的时间戳,转化成给定的日期格式
    将毫秒转换成秒,会损失毫秒级的精度,这个损失是设计内的
    :param ts: 即将被转换的时间戳数字
    :param format_string: 转换后的日期格式,默认是: 2022-01-01 10:00:00的格式
    :return: 转化完成后的日期字符串
    """
    # 将13位的时间戳 规范为10位时间戳
    ts10 = int(ts / 1000)
    # 调用ts10_to_date_str方法完成转换即可
    return ts10_to_date_str(ts10, format_string=format_string)
