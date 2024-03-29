# -*- coding: utf-8 -*-
# 使用utf-8编码


# 引入数据类
from diagnose_data import DiagnoseData
# 引入日志库
import logging
# 引入pandas库，进行数据筛选、清洗
# 非标准库，需要安装
import pandas


# 类DiagnoseFunction1
class DiagnoseFunction1:

    # 构造函数，传入DiagnoseData类实例
    def __init__(self, data):
        self.__data = data

    # 功能1辅助函数，用于将两张表合并
    # tb1_idx: 表1序号
    # tb1_cols: 表1取的数据列
    # tb1_col_names: 表1关键字列数组（字符串数组）
    # tb2_idx: 表2序号
    # tb2_col_names: 表1关键字列数组（字符串数组）
    # 从表1的临时表中取数据，与表2的原始表进行合并，产物为表2的临时表
    def __merge(self, tb1_idx, tb1_cols, tbl_col_names, tb2_idx, tb2_col_names):
        logging.info('开始读取' + self.__data.SHORT_FILE_NAMES[tb1_idx])
        df_tb1_tmp = pandas.read_csv(self.__data.Function1Files[tb1_idx], usecols=tb1_cols)
        # 先去重，降低计算量
        df_tb1_tmp = df_tb1_tmp.drop_duplicates(tbl_col_names)

        logging.info('开始读取' + self.__data.SHORT_FILE_NAMES[tb2_idx])
        df_tb2_tmp = pandas.read_csv(self.__data.RawPath[tb2_idx])
        logging.info('原始' + self.__data.SHORT_FILE_NAMES[tb2_idx] + '行数' + str(df_tb2_tmp.shape[0]))

        df_merge = pandas.merge(df_tb1_tmp, df_tb2_tmp, left_on=tbl_col_names, right_on=tb2_col_names)
        logging.info('合并文件< ' + self.__data.Function1Files[tb2_idx] + ' > 行数：' + str(df_merge.shape[0]))

        df_merge.to_csv(self.__data.Function1Files[tb2_idx], index=False)
        logging.info('合并' + self.__data.SHORT_FILE_NAMES[tb2_idx] + '文件完成')

    # 功能1检索1，筛选drug表，生成f1_dataselect20XXqX.csv，最后转为dataselect表
    def __retrieve1(self):
        logging.info('开始功能1检索1')
        # 读取drug文件，并将drugname列转换成小写，将列role_cod列转换成大写，所有列均取出
        df_drug = pandas.read_csv(self.__data.RawPath[DiagnoseData.DRUG],
                                  converters={
                                      'drugname': str.lower,
                                      'role_cod': str.upper
                                  }
                                  )
        logging.info('drug原始文件行数: ' + str(df_drug.shape[0]))

        logging.info('开始筛选drug')
        # 先筛选出非空，以免后续报错
        df_drug = df_drug[(df_drug['drugname'].notnull()) & (df_drug['role_cod'].notnull())]
        # 筛选drugname为risperidone，role_cod为RS或SS
        df_drug = df_drug[(df_drug['drugname'].str.contains('risperidone'))
                          & ((df_drug['role_cod'].str.match('SS')) | (df_drug['role_cod'].str.match('PS')))]
        # 两个条件可以合并，但代码阅读性太差

        # 保存筛选后的文件
        df_drug.to_csv(self.__data.Function1Files[DiagnoseData.DATASELECT], index=False)
        logging.info('drug筛选后文件< ' + self.__data.Function1Files[DiagnoseData.DATASELECT]
                     + ' > 行数：' + str(df_drug.shape[0]))
        logging.info('功能1检索1执行完毕\n')

    # 功能1检索2，生成最终的f1_drug20XXqX.csv
    def __retrieve2(self):
        logging.info('开始功能1检索2')
        self.__merge(DiagnoseData.DATASELECT,
                     [1],
                     ['caseid'],
                     DiagnoseData.DRUG,
                     ['caseid']
                     )
        logging.info('功能1检索2执行完毕\n')

    # 功能1检索3，生成f1_indi20XXqX.csv
    def __retrieve3(self):
        logging.info('开始功能1检索3')
        self.__merge(DiagnoseData.DATASELECT,
                     [1, 2],
                     ['caseid', 'drug_seq'],
                     DiagnoseData.INDI,
                     ['caseid', 'indi_drug_seq']
                     )
        logging.info('功能1检索3执行完毕\n')

    # 功能1检索4，生成f1_ther20XXqX.csv
    def __retrieve4(self):
        logging.info('开始功能1检索4')
        self.__merge(DiagnoseData.DATASELECT,
                     [1, 2],
                     ['caseid', 'drug_seq'],
                     DiagnoseData.THER,
                     ['caseid', 'dsg_drug_seq']
                     )
        logging.info('功能1检索4执行完毕\n')

    # 功能1检索5，生成f1_demo20XXqX.csv
    def __retrieve5(self):
        logging.info('开始功能1检索5')
        self.__merge(DiagnoseData.DATASELECT,
                     [1],
                     ['caseid'],
                     DiagnoseData.DEMO,
                     ['caseid']
                     )
        logging.info('功能1检索5执行完毕\n')

    # 功能1检索6，生成f1_outc20XXqX.csv
    def __retrieve6(self):
        logging.info('开始功能1检索6')
        self.__merge(DiagnoseData.DATASELECT,
                     [1],
                     ['caseid'],
                     DiagnoseData.OUTC,
                     ['caseid']
                     )
        logging.info('功能1检索6执行完毕\n')

    # 功能1检索7，生成f1_reac20XXqX.csv
    def __retrieve7(self):
        logging.info('开始功能1检索7')
        self.__merge(DiagnoseData.DATASELECT,
                     [1],
                     ['caseid'],
                     DiagnoseData.REAC,
                     ['caseid']
                     )
        logging.info('功能1检索7执行完毕\n')

    # 功能1检索8，生成f1_rpsr20XXqX.csv
    def __retrieve8(self):
        logging.info('开始功能1检索8')
        self.__merge(DiagnoseData.DATASELECT,
                     [1],
                     ['caseid'],
                     DiagnoseData.RPSR,
                     ['caseid']
                     )
        logging.info('功能1检索8执行完毕\n')

    # 功能1入口，前序文件生成好后，某些步骤可以跳过
    def analyse(self):
        logging.info('开始进行功能1分析\n')
        self.__retrieve1()
        self.__retrieve2()
        self.__retrieve3()
        self.__retrieve4()
        self.__retrieve5()
        self.__retrieve6()
        self.__retrieve7()
        self.__retrieve8()
        logging.info('功能1分析完毕\n')
