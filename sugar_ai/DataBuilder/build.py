import pandas as pd
from base import DBFile, DBSQL

class DataBuilder(Base):
    def __init__(self, source_type: str="postgresql"):
        self.handler = None
        self.source_type = source_type
        
        # 根据类型初始化对应的handler
        if source_type == "file":
            self.handler = DBFile()
        elif source_type == "postgresql":
            self.handler = DBSQL()
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    def save_data(self, df: pd.DataFrame, table: str) -> None:
        self.handler.save_data(df, table)

    def read_data(self, table: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.handler.read_data(table=table, start_date=start_date, end_date=end_date)
