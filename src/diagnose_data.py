# -*- coding: utf-8 -*-
# 使用utf-8编码


# 引入操作系统库，检查文件是否存在
import os
# 引入日志库
import logging


# 诊断数据类
class DiagnoseData:
    # 静态变量 SHORT_FILE_NAMES
    # 存放需要处理的文件名，需要与私有变量拼接
    # 假定所有数据文件名都按 年份+文件类型+季度.csv 格式
    SHORT_FILE_NAMES = ['demo',
                        'drug',
                        'indi',
                        'outc',
                        'reac',
                        'rpsr',
                        'ther',
                        'dataselect']

    # 定义一组伪常量，方便获取文件名、文件类型，注意与文件短名称数量、次序一致
    DEMO = 0
    DRUG = 1
    INDI = 2
    OUTC = 3
    REAC = 4
    RPSR = 5
    THER = 6
    DATASELECT = 7
    # 文件数量，使用最后一个文件序号加1
    FILE_NUM = DATASELECT + 1

    # 构造函数，假定所有数据文件命名规则一致
    # directory: 待分析的目录，字符串，如'd:\\data\\2018q1'，末尾不带'\'
    # year: 年份，字符串，如'2018'
    # quarter: 季度，字符串，如'q1'
    # 传入目录的目的，防止不同年份季度的数据混在一起
    def __init__(self, directory, year, quarter):
        # 成员变量
        self.DataDir = directory
        self.DataYear = year
        self.DataQuarter = quarter
        # 存放要分析的原始文件全路径
        self.RawPath = []
        # 存放功能1步骤生成的临时文件，文件名格式为数据文件名前面加上f1，f代表function
        self.Function1Files = []
        # 存放功能2步骤生成的临时文件，文件名格式为数据文件名前面加上f2，f代表function
        self.Function2Files = []
        # 存放功能3步骤生成的临时文件，文件名格式为数据文件名前面加上f3，f代表function
        self.Function3Files = []
        # 拼接全路径
        for short_name in DiagnoseData.SHORT_FILE_NAMES:
            self.RawPath.append(directory + '\\' + short_name + year + quarter + '.csv')
            self.Function1Files.append(directory + '\\result\\' + 'f1_' + short_name + year + quarter + '.csv')
            self.Function2Files.append(directory + '\\result\\' + 'f2_' + short_name + year + quarter + '.csv')
            self.Function3Files.append(directory + '\\result\\' + 'f3_' + short_name + year + quarter + '.csv')

    # 检查数据文件是否存在
    # 暂时对文件中列名、列次序未做检查，假定保持不变
    def check_files_exist(self):
        for file_path in self.RawPath:
            # dataseelect类文件不做检查，待生成
            if 'dataselect' in file_path:
                continue
            if not os.path.isfile(file_path):
                logging.error('原始数据文件< ' + file_path + ' >不存在')
                return False
            else:
                logging.info('原始数据文件< ' + file_path + ' >检查ok')

        # 检查数据目录下result目录是否存在
        result_path = self.DataDir + '\\result'
        if not os.path.exists(result_path):
            # 如不存在，尝试创建
            os.mkdir(result_path)
            if not os.path.exists(result_path):
                logging.error('无法创建目录< ' + result_path + ' >')
                return False
        return True
