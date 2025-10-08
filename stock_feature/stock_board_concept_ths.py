from akshare.stock_feature.stock_board_concept_ths import stock_board_concept_name_ths,stock_board_concept_info_ths,stock_board_concept_index_ths,stock_board_concept_summary_ths
from utils.datasaver import DataManager
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, MetaData, Table, BigInteger, \
    Boolean, Date
import time
import pandas as pd

def save_stock_board_concept_name_ths_df():
    #获取同花顺概念板块代码和名称字典并保存
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



def save_stock_board_concept_index_ths_df(start_date="20250930", end_date="20250930"):
    '''
    同花顺-板块-概念板块-指数数据
    :param start_date:
    :param end_date:
    :return:
    '''
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

        stock_board_concept_index_ths_df_column_types = {
            'code': String(50),
            'name': String(50),
            'timestamp': DateTime,
            'open': Float,
            'high': Float,
            'low': Float,
            'close': Float,
            'volume': Float,
            'turnover': Float,
        }

        stock_board_concept_index_ths_df['code'] = stock_board_concept_name_ths_df['code'][i]
        stock_board_concept_index_ths_df['name'] = stock_board_concept_name_ths_df['name'][i]

        # 将code和name列移到最前面
        cols = stock_board_concept_index_ths_df.columns.tolist()
        cols = ['code', 'name'] + [col for col in cols if col not in ['code', 'name']]
        stock_board_concept_index_ths_df = stock_board_concept_index_ths_df[cols]

        data_manager = DataManager()

        # 方法1: 使用自动ID生成（code + timestamp）
        data_manager.save_dataframe(
            df=stock_board_concept_index_ths_df,
            table_name='stock_board_concept_index_ths',
            id_column='code',  # 会自动与timestamp组合成唯一ID
            timestamp_column='timestamp',
            data_type='concept_index_ths',
            column_types=stock_board_concept_index_ths_df_column_types
        )

        # # 方法2: 使用自定义ID生成器（更灵活）
        # def custom_id_generator(row, index):
        #     # 使用code和timestamp生成唯一ID
        #     timestamp_str = row['timestamp'].strftime('%Y%m%d') if pd.notna(row['timestamp']) else 'nodate'
        #     return f"{row['code']}_{timestamp_str}"
        #
        # data_manager.save_dataframe(
        #     df=stock_board_concept_index_ths_df,
        #     table_name='stock_board_concept_index_ths',
        #     id_generator=custom_id_generator,  # 使用自定义ID生成器
        #     timestamp_column='timestamp',
        #     data_type='concept_index_ths',
        #     column_types=stock_board_concept_index_ths_df_column_types
        # )

        time.sleep(1)

def save_stock_board_concept_summary_ths_df():
    '''
    同花顺概念日期
    :return:
    '''
    stock_board_concept_name_ths_df = stock_board_concept_name_ths()
    #  日期          概念名称  驱动事件 龙头股 成分股数量
    stock_board_concept_summary_ths_df = stock_board_concept_summary_ths()
    print(stock_board_concept_summary_ths_df)
    stock_board_concept_summary_ths_df = stock_board_concept_summary_ths_df.rename(
        columns={
            '日期': 'timestamp',
            '概念名称': 'name',
            '驱动事件': 'catalyst',
            '龙头股': 'market_leader',
            '成分股数量': 'num',
        }
    )

    stock_board_concept_summary_ths_df_column_types = {
        'code': String(50),
        'name': String(250),
        'timestamp': DateTime,
        'catalyst': Text,
        'market_leader':  String(250),
        'num': Float,
    }
    stock_board_concept_summary_ths_df = stock_board_concept_summary_ths_df.merge(
        stock_board_concept_name_ths_df[['name', 'code']],
        on='name',
        how='left'
    )

    # 将code和name列移到最前面
    cols = stock_board_concept_summary_ths_df.columns.tolist()
    cols = ['code','name'] + [col for col in cols if col not in ['code','name']]
    stock_board_concept_summary_ths_df = stock_board_concept_summary_ths_df[cols]

    data_manager = DataManager()

    # 方法1: 使用自动ID生成（code + timestamp）
    data_manager.save_dataframe(
        df=stock_board_concept_summary_ths_df,
        table_name='stock_board_concept_summary_ths',
        id_column='code',  # 会自动与timestamp组合成唯一ID
        timestamp_column='timestamp',
        data_type='concept_summary_ths',
        column_types=stock_board_concept_summary_ths_df_column_types
    )


if __name__ == "__main__":


    # stock_board_concept_info_ths_df = stock_board_concept_info_ths(
    #     symbol="阿里巴巴概念"
    # )
    # print(stock_board_concept_info_ths_df)

    # save_stock_board_concept_index_ths_df('2020-01-01','2025-09-30')

    save_stock_board_concept_summary_ths_df()