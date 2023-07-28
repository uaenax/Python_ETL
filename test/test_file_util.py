"""
针对file_util.py内的方法做单元测试
"""
import os.path
from unittest import TestCase
from util import file_util


class TestFileUtil(TestCase):
    def setUp(self) -> None:
        pass
        self.project_root_path = os.path.dirname(os.getcwd())

    def test_get_dir_files_list(self):
        """
               请在工程根目录的test文件夹内建立：
               test_dir/
                   inner1/
                       3
                       4
                       inner2/
                           5
                   1
                   2
               的目录结构用于进行此方法的单元测试
               不递归结果应该是1和2
               递归结果应该是1, 2, 3, 4, 5
               """
        test_path = f'{self.project_root_path}/test_dir'

        # 先测试不递归
        result = file_util.get_dir_files_list(test_path, recursion=False)
        # result记录的是绝对路径,我们需要将文件的名字取出来
        names = []  # 定义一个list记录结果的文件名
        for i in result:
            # os.path.basename可以从路径中取出最后的文件名
            names.append(os.path.basename(i))
        # 为了避免结果的顺序产生测试失败,将names对象升序
        names.sort()
        self.assertEqual(['1', '2'], names)

        # 再测试递归
        result = file_util.get_dir_files_list(test_path, recursion=True)
        # result记录的是绝对路径，我们需要将文件的名字取出来
        names = []  # 定义一个list记录结果的文件名
        for i in result:
            # os.path.basename可以从路径中取出最后的文件名
            names.append(os.path.basename(i))
        names.sort()
        self.assertEqual(["1", "2", "3", "4", "5"], names)

    def test_new_by_compare_list(self):
        """测试new_by_compare_list方法"""
        a_list = ['e:/a.txt', 'e:/b.txt']
        b_list = ['e:/a.txt', 'e:/b.txt', 'e:/c.txt', 'e:/d.txt']
        result = file_util.get_new_by_compare_list(a_list, b_list)
        self.assertEqual(['e:/c.txt', 'e:/d.txt'], result)
