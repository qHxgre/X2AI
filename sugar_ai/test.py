
class DBFile:
    """通用文件数据库基类，处理基于目录结构的文件存储"""
    
    DEFAULT_PARTITION_FIELD = "partitionBy"
    
    def __init__(self, base_path: Optional[str] = None):
        """
        初始化文件数据库
        
        :param base_path: 数据库根目录路径，如果为None则使用项目目录下的DataBase
        """
        if base_path is None:
            self.base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "DataBase")
        else:
            self.base_path = base_path
            
        os.makedirs(self.base_path, exist_ok=True)

    def save_data(
        self,
        data: pd.DataFrame,
        table: str,
        keys: List[str],
    ) -> None:
        """
        将DataFrame保存为Parquet格式，支持分区存储和去重
        
        Args:
            data: 要存储的DataFrame
            table: 表名，将作为子目录名称
            keys: 唯一键列名列表，用于去重
        """
        if not keys:
            raise ValueError("去重键列不能为空")
        
        if data.empty:
            return  # 如果数据为空，直接返回
        
        # 创建表目录
        table_path = os.path.join(self.base_path, table)
        os.makedirs(table_path, exist_ok=True)
        
        # 检查是否是分区表
        is_partitioned = self.DEFAULT_PARTITION_FIELD in data.columns
        
        if is_partitioned:
            # 分区表处理 - 按分区处理数据
            partitions = data[self.DEFAULT_PARTITION_FIELD].unique()
            
            for partition in partitions:
                # 获取当前分区的数据
                partition_data = data[data[self.DEFAULT_PARTITION_FIELD] == partition]
                
                # 构建分区路径
                partition_path = os.path.join(
                    table_path, 
                    f"{self.DEFAULT_PARTITION_FIELD}={partition}"
                )
                os.makedirs(partition_path, exist_ok=True)
                
                # 1. 尝试读取现有分区数据
                existing_data = None
                parquet_file = os.path.join(partition_path, "data.parquet")
                if os.path.exists(parquet_file):
                    try:
                        existing_data = pq.read_table(parquet_file).to_pandas()
                    except Exception as e:
                        print(f"读取分区数据失败: {e}")
                
                # 2. 合并新旧数据并去重
                if existing_data is not None and not existing_data.empty:
                    # 合并新旧数据
                    combined = pd.concat([existing_data, partition_data], ignore_index=True)
                    # 按keys去重，保留最后出现的记录
                    partition_data = combined.drop_duplicates(subset=keys, keep="last")
                else:
                    # 只有新数据，只需简单去重
                    partition_data = partition_data.drop_duplicates(subset=keys, keep="last")
                
                # 转换为Arrow Table
                arrow_table = pa.Table.from_pandas(partition_data)
                
                # 3. 保存分区数据
                pq.write_table(
                    table=arrow_table,
                    where=parquet_file,
                )
        else:
            # 非分区表处理
            parquet_file = os.path.join(table_path, "data.parquet")
            
            # 1. 尝试读取现有数据
            existing_data = None
            if os.path.exists(parquet_file):
                try:
                    existing_data = pq.read_table(parquet_file).to_pandas()
                except Exception as e:
                    print(f"读取数据失败: {e}")
            
            # 2. 合并新旧数据并去重
            if existing_data is not None and not existing_data.empty:
                # 合并新旧数据
                combined = pd.concat([existing_data, data], ignore_index=True)
                # 按keys去重，保留最后出现的记录
                data = combined.drop_duplicates(subset=keys, keep="last")
            else:
                # 只有新数据，只需简单去重
                data = data.drop_duplicates(subset=keys, keep="last")
            
            # 转换为Arrow Table
            arrow_table = pa.Table.from_pandas(data)
            
            # 3. 保存数据
            pq.write_table(
                table=arrow_table,
                where=parquet_file,
            )
        
        # 保存元数据
        self._save_metadata(
            table_name=table,
            keys=keys,
            partition_cols=[self.DEFAULT_PARTITION_FIELD] if is_partitioned else None,
            schema=arrow_table.schema
        )

    def read_data(
        self,
        table: str,
        filters: Optional[Dict[str, List[Any]]] = None,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        从Parquet文件中读取数据
        
        Args:
            table: 表名
            filters: 过滤条件字典，支持两种格式:
                    1. {"date": [start_date, end_date]} - 范围筛选
                    2. {"age": [45, 46]} - 多值筛选
            columns: 要读取的列名列表，None表示读取所有列
            
        Returns:
            读取的DataFrame
        """
        table_path = os.path.join(self.base_path, table)
        
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table {table} not found")
        
        # 读取元数据
        metadata = self._load_metadata(table)
        
        # 转换筛选条件为PyArrow格式
        arrow_filters = self._convert_filters(filters) if filters else None
        
        # 检查是否是分区表
        if metadata['partition_cols']:
            dataset = ds.dataset(
                source=table_path,
                format="parquet",
                partitioning=[self.DEFAULT_PARTITION_FIELD]
            )
            scanner = dataset.scanner(filter=arrow_filters, columns=columns)
            table = scanner.to_table()
        else:
            table = pq.read_table(
                os.path.join(table_path, "data.parquet"),
                filters=arrow_filters,
                columns=columns
            )
        
        df = table.to_pandas()
        if self.DEFAULT_PARTITION_FIELD in df.columns:
            return df.drop(self.DEFAULT_PARTITION_FIELD, axis=1)
        else:
            return df
    
    def _convert_filters(
        self, 
        filters: Dict[str, Union[Dict[str, Any], List[Any]]]
    ) -> pc.Expression:
        """
        将字典形式的筛选条件转换为PyArrow表达式
        
        Args:
            filters: 过滤条件字典
            
        Returns:
            PyArrow表达式对象
        """
        expressions = []
        
        for column, condition in filters.items():
            if column == "date" and isinstance(condition, list):
                # 处理日期范围筛选 [start, end]
                if len(condition) == 2:
                    expr = (pc.field(column) >= pd.to_datetime(condition[0])) & (pc.field(column) <= pd.to_datetime(condition[1]))
                    expressions.append(expr)
                elif len(condition) == 1:
                    expressions.append(pc.field(column) >= pd.to_datetime(condition[0]))
                else:
                    raise ValueError("时间筛选参数错误！")
            elif isinstance(condition, list):
                # 处理多值筛选 [val1, val2, ...]
                if condition:
                    expressions.append(pc.field(column).isin(condition))
            elif isinstance(condition, dict):
                # 处理范围筛选 {"start": x, "end": y}
                expr = None
                if "start" in condition:
                    expr = pc.field(column) >= condition["start"]
                if "end" in condition:
                    end_expr = pc.field(column) <= condition["end"]
                    expr = end_expr if expr is None else expr & end_expr
                if expr is not None:
                    expressions.append(expr)
            else:
                # 处理单值筛选
                expressions.append(pc.field(column) == condition)
        
        # 将所有表达式用AND连接
        if not expressions:
            return None
        elif len(expressions) == 1:
            return expressions[0]
        else:
            # 使用reduce和&运算符组合所有表达式
            return reduce(lambda x, y: x & y, expressions)

    def _save_metadata(
        self,
        table_name: str,
        keys: List[str],
        partition_cols: Optional[List[str]],
        schema: pa.Schema
    ) -> None:
        """保存表的元数据"""
        metadata = {
            "table_name": table_name,
            "keys": keys,
            "partition_cols": partition_cols,
            "schema": {
                "names": schema.names,
                "types": [str(field.type) for field in schema]
            },
            "created_at": pd.Timestamp.now().isoformat()
        }
        
        metadata_path = os.path.join(self.base_path, table_name, "_metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

    def _load_metadata(self, table_name: str) -> Dict[str, Any]:
        """加载表的元数据"""
        metadata_path = os.path.join(self.base_path, table_name, "_metadata.json")
        
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(f"Metadata for table {table_name} not found")
        
        with open(metadata_path, 'r') as f:
            return json.load(f)

    def list_tables(self) -> List[str]:
        """列出所有表名"""
        return [d for d in os.listdir(self.base_path) 
                if os.path.isdir(os.path.join(self.base_path, d)) and not d.startswith('_')]

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return os.path.exists(os.path.join(self.base_path, table_name))
