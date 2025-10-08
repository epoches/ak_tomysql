# -*- coding: utf-8 -*-
import os
from pathlib import Path
# zmbs home dir
AKMYSQL_HOME = os.environ.get('AKMYSQL_HOME')
if not AKMYSQL_HOME:
    AKMYSQL_HOME = os.path.abspath(os.path.join(Path.home(), 'akmysql-home'))

# 股票类交易手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
close_tax = 0.001
open_commission = 0.0003
close_commission = 0.0003
min_commission = 5

# csv保存路径
CSV_HOME = os.environ.get('CSV_HOME')
if not CSV_HOME:
    CSV_HOME = os.path.abspath(os.path.join(AKMYSQL_HOME, 'csvs'))

# csv保存路径
JPG_HOME = os.environ.get('JPG_HOME')
if not JPG_HOME:
    JPG_HOME = os.path.abspath(os.path.join(AKMYSQL_HOME, 'jpgs'))
