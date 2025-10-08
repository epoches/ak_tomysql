# windows 计划任务每周一到周五执行即可
from  stock_feature.stock_board_concept_ths import save_stock_board_concept_name_ths_df,save_stock_board_concept_index_ths_df
from datetime import datetime
import calendar

if __name__ == "__main__":

    start_date = datetime.now().strftime('%Y%m%d')
    end_date = datetime.now().strftime('%Y%m%d')
    today = datetime.datetime.now()

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
        save_stock_board_concept_name_ths_df()
    # 保存 同花顺概念日线数据
    save_stock_board_concept_index_ths_df(start_date,end_date)

    # 陆续添加中


