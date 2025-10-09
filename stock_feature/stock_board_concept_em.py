
from akshare.stock.stock_board_concept_em import stock_board_concept_name_em,stock_board_concept_spot_em,stock_board_concept_hist_em,stock_board_concept_hist_min_em,stock_board_concept_cons_em
from utils.datasaver import DataManager
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, MetaData, Table, BigInteger, \
    Boolean, Date
import time
from datetime import datetime
import akshare as ak

# 设置代理
proxies = {
    "http": "http://your_proxy_ip:proxy_port",
    "https": "http://your_proxy_ip:proxy_port",
}

# 在使用akshare函数时传递proxies参数
# stock_board_concept_em_df = ak.stock_board_concept_em(proxies=proxies)

def save_stock_board_concept_em_df():
    stock_board_concept_em_df = stock_board_concept_name_em()
    print(stock_board_concept_em_df)
    stock_board_concept_em_df['timestamp'] = datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
    # ['排名', '板块名称', '板块代码', '最新价', '涨跌额', '涨跌幅', '总市值', '换手率', '上涨家数', '下跌家数', '领涨股票', '领涨股票-涨跌幅']
    # ranking, sector_name, sector_code, latest_price, change_amount, change_percentage, total_market_cap, turnover_rate, advancing_issues, declining_issues, leading_stock, leading_stock_change_percentage
    stock_board_concept_em_df = stock_board_concept_em_df.rename(
        columns={
            '排名': 'ranking',
            '板块名称': 'name',
            '板块代码': 'code',
            '最新价': 'latest_price',
            '涨跌额': 'change_amount',
            '涨跌幅': 'change_percentage',
            '总市值': 'total_market_cap',
            '换手率': 'turnover_rate',
            '上涨家数': 'advancing_issues',
            '下跌家数': 'declining_issues',
            '领涨股票': 'leading_stock',
            '领涨股票-涨跌幅': 'leading_stock_change_percentage',
        }
    )

    cols = stock_board_concept_em_df.columns.tolist()
    cols = ['code','name'] + [col for col in cols if col not in ['code','name']]
    stock_board_concept_em_df = stock_board_concept_em_df[cols]
    stock_board_concept_em_df_column_types = {
        'code': String(50),  #
        'name': String(250),  #
        'timestamp':DateTime,
        'ranking': Integer,
        'latest_price': Float,
        'change_amount': Float,
        'change_percentage': Float,
        'total_market_cap': Float,
        'turnover_rate': Float,
        'advancing_issues': Integer,
        'declining_issues': Integer,
        'leading_stock': String(250),
        'leading_stock_change_percentage':Float,
    }
    data_manager = DataManager()
    data_manager.save_dataframe(
        df=stock_board_concept_em_df,
        table_name='stock_board_concept_name_em',
        id_column='code',
        timestamp_column='timestamp',
        data_type='concept_name_em',
        column_types=stock_board_concept_em_df_column_types
    )

def save_stock_board_concept_cons_em_df(symbol="BK0655"):
    stock_board_concept_cons_em_df = stock_board_concept_cons_em(symbol=symbol)
    print(stock_board_concept_cons_em_df)
    stock_board_concept_cons_em_df['timestamp'] = datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
    stock_board_concept_cons_em_df['sectorcode'] = symbol
    # <class 'list'>: ['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '换手率', '市盈率-动态', '市净率']
    # serial_number, code, name, latest_price, change_percentage, change_amount, volume, turnover, amplitude, high, low, open, previous_close, turnover_rate, pe_ratio_dynamic, pb_ratio
    stock_board_concept_cons_em_df = stock_board_concept_cons_em_df.rename(
        columns={
            '序号': 'serial_number',
            '名称': 'name',
            '代码': 'code',
            '最新价': 'latest_price',
            '涨跌幅': 'change_percentage',
            '涨跌额': 'change_amount',
            '成交量': 'volume',
            '成交额': 'turnover',
            '振幅': 'amplitude',
            '最高': 'high',
            '最低': 'low',
            '今开': 'open',
            '昨收': 'previous_close',
            '换手率': 'turnover_rate',
            '市盈率-动态': 'pe_ratio_dynamic',
            '市净率': 'pb_ratio',
        }
    )

    cols = stock_board_concept_cons_em_df.columns.tolist()
    cols = ['code','name'] + [col for col in cols if col not in ['code','name']]
    stock_board_concept_em_df = stock_board_concept_cons_em_df[cols]
    # latest_price, change_percentage, change_amount, volume, turnover, amplitude, high, low, open, previous_close, turnover_rate, pe_ratio_dynamic, pb_ratio
    stock_board_concept_cons_em_df_column_types = {
        'code': String(50),  #
        'name': String(250),  #
        'sectorcode': String(50),
        'timestamp':DateTime,
        'serial_number': Integer,
        'latest_price': Float,
        'change_percentage': Float,
        'change_amount': Float,
        'volume': Float,
        'turnover': Float,
        'amplitude': Float,
        'high': Float,
        'low': String(250),
        'open':Float,
        'previous_close': Float,
        'turnover_rate': Float,
        'pe_ratio_dynamic': Float,
        'pb_ratio': Float,
    }
    data_manager = DataManager()
    data_manager.save_dataframe(
        df=stock_board_concept_em_df,
        table_name='stock_board_concept_cons_em',
        id_column='code',
        timestamp_column='timestamp',
        data_type='conceptcons_em',
        column_types=stock_board_concept_cons_em_df_column_types
    )



if __name__ == "__main__":

    # save_stock_board_concept_em_df()

    stock_board_concept_em_df = stock_board_concept_name_em()
    for code in  stock_board_concept_em_df['板块代码']:
        save_stock_board_concept_cons_em_df(code)
        time.sleep(3)