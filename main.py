# windows 计划任务每周一到周五执行即可
from akshare.stock.stock_board_concept_em import stock_board_concept_name_em
from stock_feature.stock_board_concept_ths import save_stock_board_concept_name_ths_df,process_save_stock_board_concept_index_ths_df,save_stock_board_concept_summary_ths_df
from stock_feature.stock_board_concept_em import save_stock_board_concept_em_df,process_save_stock_board_concept_cons_em_df
from datetime import datetime
import calendar
import logging
logger = logging.getLogger(__name__)
from utils  import init_log
import time

if __name__ == "__main__":
    init_log("ak_mysql.log")
    start_date = datetime.now().strftime('%Y%m%d')
    end_date = datetime.now().strftime('%Y%m%d')
    today = datetime.now()

    print(f"当前日期：{end_date}")

    # 判断是否是星期一
    # if today.weekday() == 0:
    #     print("✅ 今天是星期一")
    #     # 保存同花顺概念板块
    #     save_stock_board_concept_name_ths_df()
    # 也可以
    last_day = calendar.monthrange(today.year, today.month)[1]
    if today.day == last_day:
        print(f"月底最后一天：{end_date}")
        # 保存同花顺概念数据
        save_stock_board_concept_name_ths_df()
        # 保存 同花顺概念日期
        save_stock_board_concept_summary_ths_df()

        # 保存 同花顺概念日线数据
        process_save_stock_board_concept_index_ths_df(today.strftime('%Y%m')+'01',end_date)

    # 保存em概念数据
    save_stock_board_concept_em_df()

    time.sleep(10)

    try:
        process_save_stock_board_concept_cons_em_df()
    except KeyboardInterrupt:
        logging.getLogger().warning("Process interrupted by user")
    except Exception as e:
        logging.getLogger().error(f"Unexpected error: {e}")
    # 陆续添加中