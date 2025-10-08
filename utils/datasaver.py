# -*- coding:utf-8 -*-
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, MetaData, Table, BigInteger, \
    Boolean, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from datetime import datetime, date
from utils import akmysql_config
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建数据库引擎
engine = create_engine(
    'mysql+pymysql://' + akmysql_config["mysql_user"] + ':' +
    akmysql_config["mysql_password"] + '@' +
    akmysql_config["mysql_host"] + ':' +
    akmysql_config["mysql_port"] + '/' +
    'akmysql',
    echo=False,
    pool_pre_ping=True
)

# SQLAlchemy 基类
Base = declarative_base()

# 存储动态创建的表类
table_classes = {}


class GenericDataSaver:
    """
    通用数据保存器，支持将不同的DataFrame保存到MySQL数据库
    支持自动推断数据类型，并可自定义列类型
    """

    def __init__(self, table_name, data_type="generic", column_types=None):
        """
        :param table_name: 表名
        :param data_type: 数据类型，用于分类管理
        :param column_types: 自定义列类型映射，例如 {'price': Float, 'volume': BigInteger}
        """
        self.table_name = table_name
        self.data_type = data_type
        self.column_types = column_types or {}
        self.table_class = self._create_table_class(table_name)
        self.metadata = MetaData()

    def _create_table_class(self, table_name):
        """动态创建表类"""
        if table_name in table_classes:
            return table_classes[table_name]

        class GenericTable(Base):
            __tablename__ = table_name
            id = Column(String(255), primary_key=True)
            data_type = Column(String(50))
            created_time = Column(DateTime, default=datetime.now)
            # timestamp 列改为可选，如果没有时间数据就不创建

        table_classes[table_name] = GenericTable
        return GenericTable

    def _infer_column_types(self, df):
        """根据DataFrame推断列类型，支持更全面的类型推断"""
        type_mapping = {
            # 整数类型
            'int8': Integer,
            'int16': Integer,
            'int32': Integer,
            'int64': BigInteger,
            'uint8': Integer,
            'uint16': Integer,
            'uint32': BigInteger,
            'uint64': BigInteger,

            # 浮点类型
            'float16': Float,
            'float32': Float,
            'float64': Float,

            # 字符串和布尔类型
            'object': Text,  # 使用Text而不是String(255)避免截断
            'string': Text,
            'bool': Boolean,

            # 时间类型
            'datetime64[ns]': DateTime,
            'datetime64[ns, UTC]': DateTime,
            'timedelta64[ns]': Text,  # 时间间隔存储为文本
        }

        column_types = {}
        for col in df.columns:
            # 优先使用用户自定义的类型
            if col in self.column_types:
                column_types[col] = self.column_types[col]
                continue

            dtype = str(df[col].dtype)

            # 特殊处理：检查列内容进一步推断类型
            if dtype == 'object':
                # 对于object类型，进一步检查内容
                if not df[col].empty:
                    sample_value = df[col].iloc[0]
                    if isinstance(sample_value, datetime):
                        column_types[col] = DateTime
                    elif isinstance(sample_value, date):
                        column_types[col] = Date
                    elif isinstance(sample_value, bool):
                        column_types[col] = Boolean
                    elif isinstance(sample_value, (int, float)):
                        # 如果内容是数字但类型是object，可能是字符串形式的数字
                        try:
                            pd.to_numeric(df[col])
                            # 检查数值范围决定使用Integer还是BigInteger
                            if df[col].apply(lambda x: pd.to_numeric(x, errors='coerce')).max() > 2147483647:
                                column_types[col] = BigInteger
                            else:
                                column_types[col] = Integer
                        except:
                            column_types[col] = Text
                    else:
                        # 检查字符串长度决定使用String还是Text
                        max_length = df[col].astype(str).str.len().max()
                        if max_length <= 255:
                            column_types[col] = String(255)
                        else:
                            column_types[col] = Text
                else:
                    column_types[col] = Text
            else:
                column_types[col] = type_mapping.get(dtype, Text)

        return column_types

    def _create_or_update_table(self, df, timestamp_column=None):
        """创建或更新表结构"""
        # 检查表是否存在
        insp = inspect(engine)
        table_exists = insp.has_table(self.table_name)

        # 推断列类型
        column_types = self._infer_column_types(df)

        if not table_exists:
            # 创建新表
            self._create_new_table(df, column_types, timestamp_column)
        else:
            # 更新现有表，添加缺失的列
            self._add_missing_columns(column_types)

    def _create_new_table(self, df, column_types, timestamp_column):
        """创建新表"""
        # 动态创建表类
        class_dict = {
            '__tablename__': self.table_name,
            '__table_args__': {'extend_existing': True},
            'id': Column(String(255), primary_key=True),
            'data_type': Column(String(50)),
            'created_time': Column(DateTime, default=datetime.now)
        }

        # 添加时间戳列（如果提供了时间戳列）
        if timestamp_column and timestamp_column in df.columns:
            class_dict['timestamp'] = Column(DateTime)

        # 添加数据列
        for col_name, col_type in column_types.items():
            if col_name not in ['id', 'timestamp', 'created_time', 'data_type']:
                class_dict[col_name] = Column(col_type)

        # 动态创建类
        new_table_class = type(
            f'DynamicTable_{self.table_name}',
            (Base,),
            class_dict
        )

        # 替换原来的表类
        table_classes[self.table_name] = new_table_class
        self.table_class = new_table_class

        # 创建表
        Base.metadata.create_all(engine, tables=[new_table_class.__table__])
        logger.info(f"成功创建表: {self.table_name}")

    def _add_missing_columns(self, column_types):
        """向现有表添加缺失的列"""
        insp = inspect(engine)
        existing_columns = {col['name'] for col in insp.get_columns(self.table_name)}

        # 添加缺失的列
        with engine.begin() as conn:
            for col_name, col_type in column_types.items():
                if col_name not in existing_columns and col_name not in ['id', 'timestamp', 'created_time',
                                                                         'data_type']:
                    self._add_column_to_table(conn, col_name, col_type)

    def _add_column_to_table(self, conn, col_name, col_type):
        """向表添加单个列"""
        # 映射SQLAlchemy类型到MySQL类型
        type_mapping = {
            Integer: "INTEGER",
            BigInteger: "BIGINT",
            Float: "FLOAT",
            String: "VARCHAR(255)",
            Text: "TEXT",
            DateTime: "DATETIME",
            Date: "DATE",
            Boolean: "BOOLEAN"
        }

        mysql_type = type_mapping.get(type(col_type), "TEXT")
        if isinstance(col_type, String) and col_type.length:
            mysql_type = f"VARCHAR({col_type.length})"

        # 处理MySQL保留字
        safe_col_name = f"`{col_name}`" if self._is_mysql_reserved_word(col_name) else col_name

        alter_sql = f"ALTER TABLE `{self.table_name}` ADD COLUMN {safe_col_name} {mysql_type}"
        try:
            conn.execute(text(alter_sql))
            logger.info(f"成功添加列: {col_name} 类型: {mysql_type}")
        except Exception as e:
            logger.error(f"添加列 {col_name} 时出错: {e}")

    def _is_mysql_reserved_word(self, word):
        """检查是否是MySQL保留字"""
        reserved_words = {
            'change', 'select', 'insert', 'delete', 'update', 'where', 'table',
            'database', 'index', 'key', 'values', 'set', 'from', 'group', 'order'
        }
        return word.lower() in reserved_words

    def save_dataframe(self, df, id_column=None, timestamp_column=None):
        """
        保存DataFrame到数据库

        :param df: 要保存的DataFrame
        :param id_column: 用作ID的列名，如果为None则自动生成
        :param timestamp_column: 时间戳列名，如果为None则不保存时间戳
        """
        if df is None or df.empty:
            logger.warning("DataFrame为空，跳过保存")
            return

        # 创建或更新表结构
        self._create_or_update_table(df, timestamp_column)

        # 准备数据
        data_list = []
        for index, row in df.iterrows():
            data_dict = {
                'data_type': self.data_type,
                'created_time': datetime.now()
            }

            # 处理ID
            if id_column and id_column in row:
                data_dict['id'] = f"{self.table_name}_{row[id_column]}"
            else:
                data_dict['id'] = f"{self.table_name}_{index}"

            # 处理时间戳（可选）
            if timestamp_column and timestamp_column in row:
                data_dict['timestamp'] = row[timestamp_column]

            # 添加其他列数据
            for col in df.columns:
                if col not in ['id', 'timestamp', 'created_time', 'data_type']:
                    # 处理NaN值，转换为None
                    value = row[col]
                    if pd.isna(value):
                        value = None
                    data_dict[col] = value

            data_list.append(data_dict)

        # 批量插入数据
        self._bulk_insert_data(data_list)

    def _bulk_insert_data(self, data_list):
        """批量插入数据"""
        if not data_list:
            return

        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            for data_dict in data_list:
                # 处理MySQL保留字，给保留字列名添加反引号
                safe_columns = []
                for key in data_dict.keys():
                    if self._is_mysql_reserved_word(key):
                        safe_columns.append(f"`{key}`")
                    else:
                        safe_columns.append(key)

                columns = ', '.join(safe_columns)
                placeholders = ':' + ', :'.join(data_dict.keys())

                # 构建更新子句
                update_parts = []
                for key in data_dict.keys():
                    if key != 'id':
                        safe_key = f"`{key}`" if self._is_mysql_reserved_word(key) else key
                        update_parts.append(f"{safe_key}=VALUES({safe_key})")

                update_clause = ', '.join(update_parts)

                stmt = text(f"""
                    INSERT INTO `{self.table_name}` ({columns})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_clause}
                """)

                session.execute(stmt, data_dict)

            session.commit()
            logger.info(f"成功保存 {len(data_list)} 条数据到表 {self.table_name}")

        except Exception as e:
            logger.error(f"保存数据时出错: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def query_data(self, conditions=None, limit=None):
        """查询数据"""
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # 使用SQL直接查询
            where_conditions = []
            params = {}

            if conditions:
                for key, value in conditions.items():
                    safe_key = f"`{key}`" if self._is_mysql_reserved_word(key) else key
                    where_conditions.append(f"{safe_key} = :{key}")
                    params[key] = value

            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            limit_clause = f"LIMIT {limit}" if limit else ""

            query_sql = f"SELECT * FROM `{self.table_name}` WHERE {where_clause} {limit_clause}"

            result = session.execute(text(query_sql), params)
            rows = result.fetchall()

            if rows:
                # 转换为DataFrame
                columns = result.keys()
                data = [dict(zip(columns, row)) for row in rows]
                return pd.DataFrame(data)
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"查询数据时出错: {e}")
            return pd.DataFrame()
        finally:
            session.close()


class DataManager:
    """
    数据管理器，用于管理多个数据保存器
    """

    def __init__(self):
        self.savers = {}

    def get_saver(self, table_name, data_type="generic", column_types=None):
        """获取或创建数据保存器"""
        key = f"{table_name}_{data_type}"
        if key not in self.savers:
            self.savers[key] = GenericDataSaver(table_name, data_type, column_types)
        return self.savers[key]

    def save_dataframe(self, df, table_name, id_column=None, timestamp_column=None, data_type="generic",
                       column_types=None):
        """保存DataFrame到指定表"""
        saver = self.get_saver(table_name, data_type, column_types)
        saver.save_dataframe(df, id_column, timestamp_column)


# 使用示例
if __name__ == "__main__":
    # 创建数据管理器
    data_manager = DataManager()

    # 示例1: 保存你的概念数据（没有时间列）
    concept_data = pd.DataFrame({
        'name': ['阿尔茨海默概念', 'AI PC', 'AI手机'],
        'code': ['308614', '309121', '309120']
    })

    data_manager.save_dataframe(
        df=concept_data,
        table_name='stock_concepts',
        id_column='code',  # 使用code作为ID
        data_type='concept'
    )

    # 示例2: 保存股票价格数据（有时间和数值列）
    price_data = pd.DataFrame({
        'code': ['000001', '000002', '000003'],
        'exchange': ['SZSE', 'SZSE', 'SZSE'],
        'open': [10.5, 20.3, 15.7],
        'close': [10.8, 20.1, 16.2],
        'volume': [1000000, 2000000, 1500000],  # 大整数
        'date': [datetime(2024, 1, 1), datetime(2024, 1, 1), datetime(2024, 1, 1)]
    })

    # 自定义列类型
    price_column_types = {
        'volume': BigInteger,  # 大整数
        'open': Float,  # 浮点数
        'close': Float,  # 浮点数
    }

    data_manager.save_dataframe(
        df=price_data,
        table_name='stock_prices',
        id_column='code',
        timestamp_column='date',
        data_type='price',
        column_types=price_column_types
    )

    # 示例3: 保存财务数据（混合类型）
    financial_data = pd.DataFrame({
        'company_code': ['000001', '000002'],
        'company_name': ['平安银行', '万科A'],  # 长文本
        'revenue': [1000000000, 2000000000],  # 大整数
        'profit': [150000000.55, 300000000.77],  # 浮点数
        'is_listed': [True, False],  # 布尔值
        'established_date': [date(1987, 12, 22), date(1984, 5, 30)]  # 日期
    })

    financial_column_types = {
        'revenue': BigInteger,
        'profit': Float,
        'is_listed': Boolean,
        'established_date': Date,
        'company_name': String(100)  # 限制长度的字符串
    }

    data_manager.save_dataframe(
        df=financial_data,
        table_name='financial_reports',
        id_column='company_code',
        data_type='financial',
        column_types=financial_column_types
    )

    # 查询示例
    saver = data_manager.get_saver('stock_concepts')
    result_df = saver.query_data()
    print("概念数据查询结果:")
    print(result_df)