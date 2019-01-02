# -*- coding: utf-8 -*-
# 使用utf-8编码

# 使用sys库、getopt库分析命令行
import sys
import getopt
# 日志类
import logging
# 引入Diagnose类
from diagnose_data import DiagnoseData
from diagnose_function1 import DiagnoseFunction1
from diagnose_function2 import DiagnoseFunction2
from diagnose_function3 import DiagnoseFunction3
from file_merge import FileMerge


# 获取命令行参数，依次返回目录、年份、季度、开始年份、结束年份
# 可在Edit Configurations中修改命令行参数
def get_args():
    # argv[0]为程序自身名称，取后面的所有命令行参数
    # 冒号:表示后面必须有参数，如f:表示输入命令行格式为-f xxx，xxx必须有
    opts, args = getopt.getopt(sys.argv[1:], 'hd:y:q:f:s:e:')
    data_directory = ''
    data_year = ''
    data_quarter = ''
    data_func = -1
    data_start_year = -1
    data_end_year = -1
    for op, value in opts:
        if op == '-h':
            print_usage()
            sys.exit()
        elif op == '-d':
            data_directory = value
        elif op == '-y':
            data_year = value
        elif op == '-q':
            data_quarter = value
        elif op == '-f':
            data_func = int(value)
        elif op == '-s':
            data_start_year = int(value)
        elif op == '-e':
            data_end_year = int(value)

    return data_directory, data_year, data_quarter, data_func, data_start_year, data_end_year


# 打印帮助
def print_usage():
    print('使用方法：')
    print('\t方式1（单季度分析）：' + sys.argv[0] + '-d [数据文件目录] -y [数据年份] -q [数据季度] -f [功能号]')
    print('\t方式2（多年分析）：' + sys.argv[0] + '-d [数据文件目录] -s [数据开始年份] -e [数据结束年份] -f [功能号]')
    print('\t单季度全功能分析使用示例：' + sys.argv[0] + '-d d:\\data\\2018q1 -y 2018 -q q1')
    print('\t单季度单功能分析使用示例：' + sys.argv[0] + '-d d:\\data\\2018q1 -y 2018 -q q1 -f 1')
    print('\t多年全功能分析使用示例：' + sys.argv[0] + '-d d:\\data\\2016_2018 -s 2016 -e 2018')
    print('\t多年单功能分析使用示例：' + sys.argv[0] + '-d d:\\data\\2016_2018 -s 2016 -e 2018 -f 2')
    print('\t查看帮助：    ' + sys.argv[0] + ' -h')
    print('\t结果文件存放在数据文件目录下的result目录中')


# 初始化日志
def init_log(data_directory):
    # 日志格式为 时间 级别 内容
    log_format = "%(asctime)-16s [%(levelname)-8s]: %(message)s"
    logging.basicConfig(filename=data_directory + '\\' + 'run.log', level=logging.DEBUG, format=log_format)


# 多年数据分析
def analyse_multi_years(data_directory, data_start_year, data_end_year, data_func):
    # 遍历年份
    for data_year in range(data_start_year, data_end_year+1):
        # 遍历季度
        for data_quarter in range(1, 5):
            str_quarter = 'q' + str(data_quarter)
            analyse_single_quarter(data_directory, str(data_year), str_quarter, data_func)


# 单季度数据分析
def analyse_single_quarter(data_directory, data_year, data_quarter, data_func):
    diagnose_data = DiagnoseData(data_directory, data_year, data_quarter)
    if not diagnose_data.check_files_exist():
        logging.error('Diagnose验证数据' + data_year + data_quarter + '文件存在失败\n')
        return

    # 功能构造
    function1 = DiagnoseFunction1(diagnose_data)
    function2 = DiagnoseFunction2(diagnose_data)
    function3 = DiagnoseFunction3(diagnose_data)

    # 根据功能号执行
    if data_func == 1:
        function1.analyse()
    elif data_func == 2:
        function2.analyse()
    elif data_func == 3:
        function3.analyse()
    else:
        function1.analyse()
        function2.analyse()
        function3.analyse()


# 主程序入口
if __name__ == '__main__':
    # 获取命令行参数
    directory, year, quarter, func, start_year, end_year = get_args()
    # 检查参数合法性
    if len(directory) < 1:
        print_usage()
        sys.exit()

    # 初始化日志
    init_log(directory)

    # 方便定位日志开始
    logging.info('*************** 开始分析 ***************')

    # 多年数据分析
    if 0 < start_year < end_year:
        analyse_multi_years(directory, start_year, end_year, func)
        # 多年数据需要进行文件合并
        fm = FileMerge(directory)
        fm.do_merge()
    # 单季度数据分析，不再进行多文件合并
    elif len(year) > 0 and len(quarter) > 0:
        analyse_single_quarter(directory, year, quarter, func)
    else:
        print_usage()
