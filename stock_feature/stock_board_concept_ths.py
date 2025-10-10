from akshare.stock_feature.stock_board_concept_ths import stock_board_concept_name_ths,stock_board_concept_info_ths,stock_board_concept_index_ths,stock_board_concept_summary_ths
from utils.datasaver import DataManager
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, MetaData, Table, BigInteger, \
    Boolean, Date
import time
import logging
logger = logging.getLogger(__name__)
from utils  import init_log
from utils.backup_restore_log import load_processed_codes, save_processed_code,clear_processed_codes

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


def process_save_stock_board_concept_index_ths_df(start_date="20250930", end_date="20250930"):
    '''
    同花顺-板块-概念板块-指数数据（带进度保存功能）
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return:
    '''
    # 初始化日志
    init_log("process_stock_board_concept_index_ths.log")

    logger.info("Starting stock board concept index processing")

    # 加载已处理的概念板块代码
    processed_codes = load_processed_codes()

    # 获取概念板块列表
    try:
        stock_board_concept_name_ths_df = stock_board_concept_name_ths()
        logger.info(f"Successfully loaded {len(stock_board_concept_name_ths_df)} concept boards")
    except Exception as e:
        logger.error(f"Failed to load concept board list: {e}")
        raise

    # 过滤掉已处理的概念板块
    remaining_concepts = stock_board_concept_name_ths_df[
        ~stock_board_concept_name_ths_df['code'].isin(processed_codes)
    ]

    # 检查是否还有需要处理的概念板块
    if remaining_concepts.empty:
        logger.info("No remaining concept boards to process")
        clear_processed_codes()  # 清空进度文件，为下次运行做准备
        logger.info("Processed codes file cleared for next run")
        return

    logger.info(f"Found {len(remaining_concepts)} concept boards to process")

    # 处理每个剩余的概念板块
    for i, concept_row in remaining_concepts.iterrows():
        concept_code = concept_row['code']
        concept_name = concept_row['name']

        try:
            logger.info(f"Processing concept board: {concept_name} ({concept_code})")

            # 获取概念板块指数数据
            stock_board_concept_index_ths_df = stock_board_concept_index_ths(
                symbol=concept_name, start_date=start_date, end_date=end_date
            )

            # 检查数据是否为空
            if stock_board_concept_index_ths_df.empty:
                logger.warning(f"No data found for concept board: {concept_name} ({concept_code})")
                # 仍然标记为已处理，避免重复尝试
                save_processed_code(concept_code)
                continue

            # 重命名列
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

            # 定义列类型
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

            # 添加代码和名称列
            stock_board_concept_index_ths_df['code'] = concept_code
            stock_board_concept_index_ths_df['name'] = concept_name

            # 将code和name列移到最前面
            cols = stock_board_concept_index_ths_df.columns.tolist()
            cols = ['code', 'name'] + [col for col in cols if col not in ['code', 'name']]
            stock_board_concept_index_ths_df = stock_board_concept_index_ths_df[cols]

            # 保存数据
            data_manager = DataManager()
            data_manager.save_dataframe(
                df=stock_board_concept_index_ths_df,
                table_name='stock_board_concept_index_ths',
                id_column='code',  # 会自动与timestamp组合成唯一ID
                timestamp_column='timestamp',
                data_type='concept_index_ths',
                column_types=stock_board_concept_index_ths_df_column_types
            )

            # 记录成功处理的概念板块代码
            save_processed_code(concept_code)
            logger.info(f"Successfully processed concept board: {concept_name} ({concept_code})")

            # 延迟以避免服务器过载
            time.sleep(10)

        except Exception as e:
            logger.error(f"Error processing concept board {concept_name} ({concept_code}): {e}")
            # 如果服务器拒绝连接，停止处理
            if "connection" in str(e).lower() or "refused" in str(e).lower():
                logger.error("Server connection issue detected, stopping processing")
                break
            # 其他错误继续处理下一个
            else:
                # 标记为已处理以避免重复错误
                save_processed_code(concept_code)
                logger.warning(f"Marked {concept_code} as processed despite error")
                continue

    # 检查是否所有概念板块都已处理完成
    remaining_after_process = stock_board_concept_name_ths_df[
        ~stock_board_concept_name_ths_df['code'].isin(load_processed_codes())
    ]

    if remaining_after_process.empty:
        logger.info("All concept boards processed successfully")
        clear_processed_codes()  # 清空进度文件
        logger.info("Processed codes file cleared for next run")
    else:
        logger.info(f"Processing incomplete. {len(remaining_after_process)} concept boards remaining")


# 保留原函数作为单次处理函数
def save_stock_board_concept_index_ths_df(concept_code, concept_name, start_date="20250930", end_date="20250930"):
    '''
    处理单个概念板块的指数数据（不带进度保存）
    :param concept_code: 概念板块代码
    :param concept_name: 概念板块名称
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return:
    '''
    try:
        # 获取概念板块指数数据
        stock_board_concept_index_ths_df = stock_board_concept_index_ths(
            symbol=concept_name, start_date=start_date, end_date=end_date
        )

        if stock_board_concept_index_ths_df.empty:
            logger.warning(f"No data found for concept board: {concept_name} ({concept_code})")
            return False

        # 重命名列
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

        # 定义列类型
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

        # 添加代码和名称列
        stock_board_concept_index_ths_df['code'] = concept_code
        stock_board_concept_index_ths_df['name'] = concept_name

        # 将code和name列移到最前面
        cols = stock_board_concept_index_ths_df.columns.tolist()
        cols = ['code', 'name'] + [col for col in cols if col not in ['code', 'name']]
        stock_board_concept_index_ths_df = stock_board_concept_index_ths_df[cols]

        # 保存数据
        data_manager = DataManager()
        data_manager.save_dataframe(
            df=stock_board_concept_index_ths_df,
            table_name='stock_board_concept_index_ths',
            id_column='code',
            timestamp_column='timestamp',
            data_type='concept_index_ths',
            column_types=stock_board_concept_index_ths_df_column_types
        )

        return True

    except Exception as e:
        logger.error(f"Error processing single concept board {concept_name} ({concept_code}): {e}")
        return False

# def save_stock_board_concept_index_ths_df(start_date="20250930", end_date="20250930"):
#     '''
#     同花顺-板块-概念板块-指数数据
#     :param start_date:
#     :param end_date:
#     :return:
#     '''
#     stock_board_concept_name_ths_df = stock_board_concept_name_ths()
#     for i in range(len(stock_board_concept_name_ths_df)):
#         stock_board_concept_index_ths_df = stock_board_concept_index_ths(
#             symbol=stock_board_concept_name_ths_df['name'][i], start_date=start_date, end_date=end_date
#         )
#         print(stock_board_concept_index_ths_df)
#         stock_board_concept_index_ths_df = stock_board_concept_index_ths_df.rename(
#             columns={
#                 '日期': 'timestamp',
#                 '开盘价': 'open',
#                 '最高价': 'high',
#                 '最低价': 'low',
#                 '收盘价': 'close',
#                 '成交量': 'volume',
#                 '成交额': 'turnover'
#             }
#         )
#
#         stock_board_concept_index_ths_df_column_types = {
#             'code': String(50),
#             'name': String(50),
#             'timestamp': DateTime,
#             'open': Float,
#             'high': Float,
#             'low': Float,
#             'close': Float,
#             'volume': Float,
#             'turnover': Float,
#         }
#
#         stock_board_concept_index_ths_df['code'] = stock_board_concept_name_ths_df['code'][i]
#         stock_board_concept_index_ths_df['name'] = stock_board_concept_name_ths_df['name'][i]
#
#         # 将code和name列移到最前面
#         cols = stock_board_concept_index_ths_df.columns.tolist()
#         cols = ['code', 'name'] + [col for col in cols if col not in ['code', 'name']]
#         stock_board_concept_index_ths_df = stock_board_concept_index_ths_df[cols]
#
#         data_manager = DataManager()
#
#         # 方法1: 使用自动ID生成（code + timestamp）
#         data_manager.save_dataframe(
#             df=stock_board_concept_index_ths_df,
#             table_name='stock_board_concept_index_ths',
#             id_column='code',  # 会自动与timestamp组合成唯一ID
#             timestamp_column='timestamp',
#             data_type='concept_index_ths',
#             column_types=stock_board_concept_index_ths_df_column_types
#         )
#
#         # # 方法2: 使用自定义ID生成器（更灵活）
#         # def custom_id_generator(row, index):
#         #     # 使用code和timestamp生成唯一ID
#         #     timestamp_str = row['timestamp'].strftime('%Y%m%d') if pd.notna(row['timestamp']) else 'nodate'
#         #     return f"{row['code']}_{timestamp_str}"
#         #
#         # data_manager.save_dataframe(
#         #     df=stock_board_concept_index_ths_df,
#         #     table_name='stock_board_concept_index_ths',
#         #     id_generator=custom_id_generator,  # 使用自定义ID生成器
#         #     timestamp_column='timestamp',
#         #     data_type='concept_index_ths',
#         #     column_types=stock_board_concept_index_ths_df_column_types
#         # )
#
#         time.sleep(1)

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

    save_stock_board_concept_index_ths_df('2020-01-01','2025-09-30')

    process_save_stock_board_concept_index_ths_df('2025-10-09','2025-10-10')

    save_stock_board_concept_summary_ths_df()