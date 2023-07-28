"""
读取文件名
"""
import os


def get_dir_files_list(path="./", recursion=False):
    """
    判断文件夹下面,有哪些文件
    :param path: 被判断的文件夹的路径,默认当前路径
    :param recursion: 是否递归读取,默认不递归
    :return: list对象,list里面存储的是文件的路径
    """
    # os.listdir 这个API返回的是给定的path下面有哪些'''文件和文件夹'''
    # path = path.replace('\\', '/')
    dir_name = os.listdir(path)
    files = []  # 定义一个list,用来记录文件
    for dir_name in dir_name:
        absolute_path = f'{path}/{dir_name}'
        if not os.path.isdir(absolute_path):
            # 如果进来这个if,表明这个是:文件
            files.append(absolute_path)
        else:
            # 表明是文件夹
            if recursion:  # 如果recursion是True,表明要进到文件夹里面继续找文件
                files += get_dir_files_list(absolute_path, recursion=recursion)
    return files


def get_new_by_compare_list(a_list, b_list):
    """
    从两个list中进行对比
    找出B中有而A中没有的
    比如:
    A:[1]
    B:[1,2,3]
    结果是:[2,3]
    :param a_list: A数据集
    :param b_list: B数据集
    :return: 存放B中有而A中没有的数据
    """
    # 存放结果数据集
    new_list = []

    for i in b_list:
        if i not in a_list:
            new_list.append(i)

    return new_list
