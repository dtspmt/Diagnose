# -*- coding: utf-8 -*-
# 使用utf-8编码

# 引入DiagnoseData类
from diagnose_data import DiagnoseData
# 引入os
import os
# 引入日志
import logging
# 引入pandas
import pandas


# 文件合并类，合并前提是列名、列次序保持一致
class FileMerge:
    # 构造函数，传入待处理的目录
    def __init__(self, directory):
        self.__directory = directory + '\\result'

    # 辅助函数，用于合并某个功能、某个类型的文件
    # func: 字符串，如f1
    # short_file_name: DiagnoseData中定义的文件短名称
    def __merge(self, func, short_file_name):
        # 列出所有文件
        raw_files = os.listdir(self.__directory)
        target_files = []
        # 筛选出需要的文件名fX_xxxx20XXqX.csv
        file_head = func + '_' + short_file_name
        for file_name in raw_files:
            # 排除已经合并过的文件（文件名不包含年份季度）
            if len(file_name) <= len(func) + len(short_file_name) + 5:
                continue
            if file_name.startswith(file_head) and file_name.endswith('.csv'):
                target_files.append(file_name)

        # 按字符串进行排序，即将文件按日期排列
        target_files.sort()

        # 没有匹配的文件直接返回
        if len(target_files) < 1:
            return

        # 准备存放所有的df
        df_all = []

        # 遍历所有符合条件的文件
        for target_file in target_files:
            df = pandas.read_csv(self.__directory + '\\' + target_file)
            date_str = target_file[len(file_head):len(target_file) - 4]
            # 增加年份季度列
            df['year_quarter'] = date_str
            # 往后累加
            df_all.append(df)

        # 将所有df前后排列
        df_concat = pandas.concat(df_all, ignore_index=True)
        # 取出列名
        col_names = list(df_concat)
        # 删除年份季度
        col_names.remove('year_quarter')
        # 将年份季度放到第一列
        col_names.insert(0, 'year_quarter')
        # 按照新的列次序，输出到csv
        df_concat.to_csv(self.__directory + '\\' + func + '_' + short_file_name + '.csv',
                         index=False,
                         columns=col_names)

    def do_merge(self):
        logging.info('开始合并结果文件')
        # 可以单独对某个类型文件进行合并调试
        # self.__merge('f1', 'indi')
        # 遍历所有功能，功能号[1, 3]
        for func in range(1, 4):
            for file_index in range(DiagnoseData.FILE_NUM):
                logging.info('生成合并文件< f' + str(func) + '_' + DiagnoseData.SHORT_FILE_NAMES[file_index] + ' >')
                self.__merge('f' + str(func), DiagnoseData.SHORT_FILE_NAMES[file_index])
        logging.info('结果文件合并完毕\n')
