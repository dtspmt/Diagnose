# -*- coding: utf-8 -*-
# 使用utf-8编码


# 引入数据类
from diagnose_data import DiagnoseData
# 引入日志库
import logging
# 引入pandas库，进行数据筛选、清洗
# 非标准库，需要安装
import pandas


# 类DiagnoseFunction2
class DiagnoseFunction2:

    # 构造函数，传入DiagnoseData类实例
    def __init__(self, data):
        self.__data = data

    # 功能2辅助函数，用于将两张表合并
    # tb1_idx: 表1序号
    # tb1_cols: 表1取的数据列
    # tb1_col_names: 表1关键字列数组（字符串数组）
    # tb2_idx: 表2序号
    # tb2_col_names: 表1关键字列数组（字符串数组）
    # 从表1的临时表中取数据，与表2的原始表进行合并，产物为表2的临时表
    def __merge(self, tb1_idx, tb1_cols, tbl_col_names, tb2_idx, tb2_col_names):
        logging.info('开始读取' + self.__data.SHORT_FILE_NAMES[tb1_idx])
        df_tb1_tmp = pandas.read_csv(self.__data.Function2Files[tb1_idx], usecols=tb1_cols)
        # 先去重，降低计算量
        df_tb1_tmp = df_tb1_tmp.drop_duplicates(tbl_col_names)

        logging.info('开始读取' + self.__data.SHORT_FILE_NAMES[tb2_idx])
        df_tb2_tmp = pandas.read_csv(self.__data.RawPath[tb2_idx])
        logging.info(self.__data.SHORT_FILE_NAMES[tb2_idx] + '原始文件行数：' + str(df_tb2_tmp.shape[0]))

        df_merge = pandas.merge(df_tb1_tmp, df_tb2_tmp, left_on=tbl_col_names, right_on=tb2_col_names)
        logging.info('合并文件< ' + self.__data.Function2Files[tb2_idx] + ' > 行数：' + str(df_merge.shape[0]))

        df_merge.to_csv(self.__data.Function2Files[tb2_idx], index=False)
        logging.info('合并' + self.__data.SHORT_FILE_NAMES[tb2_idx] + '文件完成')

    # 功能2检索1，生成f2_reac20xxqX.csv
    def __retrieve1(self):
        logging.info('开始功能2检索1')
        # 打开reac20XXqX.csv
        # 将pt列转换成小写
        df_reac = pandas.read_csv(self.__data.RawPath[DiagnoseData.REAC],
                                  usecols=[0, 1, 2],
                                  converters={
                                      'pt': str.lower
                                  }
                                  )
        logging.info('reac原始文件行数：' + str(df_reac.shape[0]))

        logging.info('开始筛选reac')
        # 先筛选出非空，以免后续报错
        df_reac = df_reac[(df_reac['pt'].notnull())]
        # 筛选pt包含neurotoxicity
        df_reac = df_reac[(df_reac['pt'].str.contains('neurotoxicity'))]

        logging.info('筛选文件< ' + self.__data.Function2Files[DiagnoseData.REAC] + ' > 行数：' + str(df_reac.shape[0]))

        df_reac.to_csv(self.__data.Function2Files[DiagnoseData.REAC], index=False)
        logging.info('功能2检索1执行完毕\n')

    # 功能2检索2，生成f2_outc20XXqX.csv
    def __retrieve2(self):
        logging.info('开始功能2检索2')
        self.__merge(DiagnoseData.REAC,
                     [1],
                     ['caseid'],
                     DiagnoseData.OUTC,
                     ['caseid'])
        logging.info('功能2检索2执行完毕\n')

    # 功能2检索3，生成f2_demo20XXqX.csv
    def __retrieve3(self):
        logging.info('开始功能2检索3')
        self.__merge(DiagnoseData.REAC,
                     [1],
                     ['caseid'],
                     DiagnoseData.DEMO,
                     ['caseid'])
        logging.info('功能2检索3执行完毕\n')

    # 功能2检索4，生成f2_rpsr20XXqX.csv
    def __retrieve4(self):
        logging.info('开始功能2检索4')
        self.__merge(DiagnoseData.REAC,
                     [1],
                     ['caseid'],
                     DiagnoseData.RPSR,
                     ['caseid'])
        logging.info('功能2检索4执行完毕\n')

    # 功能2检索5，生成f2_drug20XXqX.csv
    def __retrieve5(self):
        logging.info('开始功能2检索5')
        # 打开f2_reac临时表格
        # 只需要第2列，注意生成的临时文件第一列为pandas自动加的主键
        logging.info('开始读取reac')
        df_reac_tmp = pandas.read_csv(self.__data.Function2Files[DiagnoseData.REAC],
                                      usecols=[1]
                                      )
        # 去重复
        df_reac_tmp = df_reac_tmp.drop_duplicates()

        logging.info('开始读取drug')
        # 打开drug20XXqX.csv
        df_drug = pandas.read_csv(self.__data.RawPath[DiagnoseData.DRUG])
        logging.info('drug原始文件行数：' + str(df_drug.shape[0]))

        # 筛选drug
        # 先筛选出非空，以免后续报错
        df_drug = df_drug[(df_drug['role_cod'].notnull())]
        # 筛选role_cod为RS或SS
        df_drug = df_drug[(df_drug['role_cod'].str.match('SS')) | (df_drug['role_cod'].str.match('PS'))]

        # 合并f2_reac和drug
        df_drug_tmp = pandas.merge(df_reac_tmp, df_drug, left_on=['caseid'], right_on=['caseid'])
        logging.info('合并文件< ' + self.__data.Function2Files[DiagnoseData.DRUG] + ' > 行数：' + str(df_drug_tmp.shape[0]))

        df_drug_tmp.to_csv(self.__data.Function2Files[DiagnoseData.DRUG], index=False)
        logging.info('合并drug文件完成')
        logging.info('功能2检索5执行完毕\n')

    # 功能2检索6，生成f2_indi20XXqX.csv
    def __retrieve6(self):
        logging.info('开始功能2检索6')
        self.__merge(DiagnoseData.DRUG,
                     [0, 2],
                     ['caseid', 'drug_seq'],
                     DiagnoseData.INDI,
                     ['caseid', 'indi_drug_seq'])
        logging.info('功能2检索6执行完毕\n')

    # 功能2检索7，生成f2_ther20XXqX.csv
    def __retrieve7(self):
        logging.info('开始功能2检索7')
        self.__merge(DiagnoseData.DRUG,
                     [0, 2],
                     ['caseid', 'drug_seq'],
                     DiagnoseData.THER,
                     ['caseid', 'dsg_drug_seq'])
        logging.info('功能2检索7执行完毕\n')

    # 功能2入口，前序文件生成好后，某些步骤可以跳过
    def analyse(self):
        logging.info('开始进行功能2分析\n')
        self.__retrieve1()
        self.__retrieve2()
        self.__retrieve3()
        self.__retrieve4()
        self.__retrieve5()
        self.__retrieve6()
        self.__retrieve7()
        logging.info('功能2分析完毕\n')
