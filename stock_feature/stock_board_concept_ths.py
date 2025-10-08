from akshare.stock_feature.stock_board_concept_ths import stock_board_concept_name_ths,stock_board_concept_info_ths,stock_board_concept_index_ths
from utils.datasaver import DataManager
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, MetaData, Table, BigInteger, \
    Boolean, Date
import time

def save_stock_board_concept_name_ths_df():
    stock_board_concept_name_ths_df = stock_board_concept_name_ths()
    print(stock_board_concept_name_ths_df)
    stock_board_concept_name_ths_df_column_types = {
        'code': String(50),  #
        'name': String(50),  #

    }
    data_manager = DataManager()
    data_manager.save_dataframe(
        df=stock_board_concept_name_ths_df,
        table_name='stock_board_concept_name_ths',
        id_column='code',
        data_type='concept_name',
        column_types=stock_board_concept_name_ths_df_column_types
    )

def save_stock_board_concept_index_ths_df(start_date="20250930",end_date="20250930"):
    stock_board_concept_name_ths_df = stock_board_concept_name_ths()
    for i in range(len(stock_board_concept_name_ths_df)):
        stock_board_concept_index_ths_df = stock_board_concept_index_ths(
            symbol=stock_board_concept_name_ths_df['name'][i], start_date=start_date, end_date=end_date
        )
        print(stock_board_concept_index_ths_df)
        stock_board_concept_index_ths_df = stock_board_concept_index_ths_df.rename(
            columns={
                '日期': 'timestamp',
                '开盘价': 'open',
                '最高价': 'high',
                '最低价': 'low',
                '收盘价': 'close',
                '成交量': 'volume',
                '成交额': 'turnover'
            }
        )
        # 日期       开盘价       最高价  ...       收盘价          成交量           成交额
        stock_board_concept_index_ths_df_column_types = {
            'code': String(50),  #
            'name': String(50),  #
            'timestamp': DateTime,
            'open': Float,  # 浮点数
            'high': Float,  # 浮点数
            'low': Float,  # 浮点数
            'close': Float,  # 浮点数
            'volume': Float,  # 浮点数
            'turnover': Float,  # 浮点数

        }
        stock_board_concept_index_ths_df['code'] = stock_board_concept_name_ths_df['code'][i]
        stock_board_concept_index_ths_df['name'] = stock_board_concept_name_ths_df['name'][i]
        # 将code和name列移到最前面
        cols = stock_board_concept_index_ths_df.columns.tolist()
        cols = ['code', 'name'] + [col for col in cols if col not in ['code', 'name']]
        stock_board_concept_index_ths_df = stock_board_concept_index_ths_df[cols]
        data_manager = DataManager()
        data_manager.save_dataframe(
            df=stock_board_concept_index_ths_df,
            table_name='stock_board_concept_index_ths',
            id_column='code',
            timestamp_column='timestamp',
            data_type='concept_index_ths',
            column_types=stock_board_concept_index_ths_df_column_types
        )
        time.sleep(1)

if __name__ == "__main__":


    # stock_board_concept_info_ths_df = stock_board_concept_info_ths(
    #     symbol="阿里巴巴概念"
    # )
    # print(stock_board_concept_info_ths_df)

    save_stock_board_concept_index_ths_df('2020-01-01','2025-09-30')