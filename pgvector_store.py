import psycopg2
import os
import pandas as pd
from typing import List, Dict, Any, Optional, Union, Tuple
from vanna.base import VannaBase
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 日志级别设置
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
VERBOSE = LOG_LEVEL.upper() in ['DEBUG', 'TRACE']


class PgVectorStore(VannaBase):
    def __init__(self, config=None):
        super().__init__(config=config)

        self.conn = psycopg2.connect(
            host=config['pgvector_host'],
            port=config['pgvector_port'],
            database=config['pgvector_db'],
            user=config['pgvector_user'],
            password=config['pgvector_password']
        )
        # 设置自动提交为False，手动控制事务
        self.conn.autocommit = False

        self.table_name = config.get("pgvector_table", "vanna_pgvector")

        self._init_table()

    def reset_table(self):
        """
        删除并重新创建向量表
        """
        try:
            with self.conn.cursor() as cur:
                # 首先尝试删除表（如果存在）
                cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                self.conn.commit()
                print(f"表 {self.table_name} 已删除")
                
                # 然后重新创建表
                self._init_table()
                print(f"表 {self.table_name} 已重新创建，向量维度为1024")
                return True
        except Exception as e:
            self.conn.rollback()
            print(f"重置表失败: {e}")
            return False

    # def generate_embedding(self, text: str) -> List[float]:
    #     return self.embed(text)

    def _init_table(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        id SERIAL PRIMARY KEY,
                        type TEXT,
                        content TEXT,
                        embedding VECTOR(1024)
                    )
                """)
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"初始化表失败: {e}")

    # def _embed(self, text: str):
    #     return self.embed(text)  # 使用 Vanna 默认 embedding 接口
    def _embed(self, text: str) -> List[float]:
        return self.generate_embedding(data=text)

    def _insert(self, data_type: str, content: str) -> str:
        try:
            embedding = self._embed(content)
            
            # 调试信息：检查嵌入向量
            if VERBOSE:
                print(f"\n===调试: 数据库插入===")
                print(f"数据类型: {data_type}")
                print(f"嵌入向量类型: {type(embedding)}")
                if isinstance(embedding, list):
                    print(f"嵌入向量长度: {len(embedding)}")
                    print(f"嵌入向量前5个元素: {embedding[:5]}")
                else:
                    print(f"警告: 嵌入向量不是列表类型! 而是 {type(embedding)}")
            else:
                print(f"[INFO] 插入数据 (类型: {data_type})")
            
            with self.conn.cursor() as cur:
                try:
                    cur.execute(
                        f"INSERT INTO {self.table_name} (type, content, embedding) VALUES (%s, %s, %s)",
                        (data_type, content, embedding)
                    )
                    if VERBOSE:
                        print("SQL插入执行成功，准备提交...")
                    self.conn.commit()
                    if VERBOSE:
                        print("事务提交成功")
                except Exception as e:
                    print(f"[ERROR] SQL执行或提交错误: {str(e)}")
                    self.conn.rollback()
                    if VERBOSE:
                        print("事务已回滚")
                    raise
            return "ok"
        except Exception as e:
            self.conn.rollback()
            print(f"[ERROR] _insert方法最终错误: {str(e)}")
            raise Exception(f"插入数据失败: {e}")
    
    def _batch_insert(self, items: List[Dict[str, Any]]) -> bool:
        """批量插入数据
        
        Args:
            items: 包含type、content和embedding的项目列表
            
        Returns:
            bool: 是否成功
        """
        if not items:
            return True
            
        print(f"[INFO] 批量插入 {len(items)} 条数据")
        
        try:
            with self.conn.cursor() as cur:
                # 构建批量插入语句
                values_part = []
                params = []
                
                for item in items:
                    values_part.append("(%s, %s, %s)")
                    params.extend([
                        item['type'], 
                        item['content'], 
                        item['embedding']
                    ])
                
                query = f"""
                    INSERT INTO {self.table_name} (type, content, embedding) 
                    VALUES {','.join(values_part)}
                """
                
                # 执行批量插入
                cur.execute(query, params)
                self.conn.commit()
                
                print(f"[INFO] 成功批量插入 {len(items)} 条数据")
                return True
                
        except Exception as e:
            self.conn.rollback()
            print(f"[ERROR] 批量插入失败: {str(e)}")
            
            # 如果批量失败，尝试逐条插入
            print("[INFO] 尝试逐条插入...")
            success_count = 0
            
            for item in items:
                try:
                    self._insert(item['type'], item['content'])
                    success_count += 1
                except Exception as item_e:
                    print(f"[ERROR] 逐条插入项目 {success_count+1} 失败: {str(item_e)}")
            
            print(f"[INFO] 逐条插入完成, 成功: {success_count}/{len(items)}")
            return success_count > 0

    def add_ddl(self, ddl: str, **kwargs) -> str:
        return self._insert("ddl", ddl)

    def add_documentation(self, doc: str, **kwargs) -> str:
        return self._insert("documentation", doc)

    def add_question_sql(self, question: str, sql: str, **kwargs) -> str:
        return self._insert("question_sql", f"{question} :: {sql}")
    
    def add_batch(self, batch: List[Dict[str, Any]], **kwargs) -> bool:
        """批量添加数据
        
        Args:
            batch: 包含要批量添加的数据项列表，每项必须有type和content字段
            
        Returns:
            bool: 是否成功
        """
        if not batch:
            return True
            
        # 准备批处理项
        items_to_insert = []
        success_count = 0
        error_count = 0
        
        # 单独处理每条记录的嵌入向量生成
        for item in batch:
            data_type = item.get('type')
            
            if data_type == 'question_sql':
                content = f"{item.get('question', '')} :: {item.get('sql', '')}"
            else:
                content = item.get('content', '')
            
            # 单独生成嵌入向量，处理可能的长度错误
            try:
                if VERBOSE:
                    print(f"[INFO] 处理 {data_type} 项目 {len(content)} 字符")
                    
                if len(content) > 2048:  # 检查文本长度是否超过API限制
                    print(f"[WARNING] 文本长度 {len(content)} 超出API限制(2048)，将被截断")
                    
                    # 打印被截断的文本的详细信息
                    print(f"[DEBUG] 原始文本的前100个字符: '{content[:100]}...'")
                    print(f"[DEBUG] 原始文本的后100个字符: '...{content[-100:]}'")
                    
                    content_truncated = content[:2048]  # 截断文本以适应API限制
                    
                    print(f"[DEBUG] 截断后文本的前100个字符: '{content_truncated[:100]}...'")
                    print(f"[DEBUG] 截断后文本的后100个字符: '...{content_truncated[-100:]}'")
                    
                    embedding = self._embed(content_truncated)
                    print(f"[INFO] 成功生成已截断文本的嵌入向量")
                else:
                    embedding = self._embed(content)
                
                # 添加到批处理列表
                items_to_insert.append({
                    'type': data_type,
                    'content': content,  # 保存原始内容，即使嵌入向量是从截断文本生成的
                    'embedding': embedding
                })
                success_count += 1
                
            except Exception as e:
                print(f"[ERROR] 为 {data_type} 项目生成嵌入向量失败: {str(e)}")
                error_count += 1
                # 继续处理下一条记录，不影响整个批次
        
        print(f"[INFO] 嵌入向量生成完成: 成功 {success_count}/{len(batch)}, 失败 {error_count}/{len(batch)}")
        
        if not items_to_insert:
            print("[WARNING] 没有成功生成嵌入向量的项目，跳过数据库插入")
            return False
            
        # 批量写入数据库
        return self._batch_insert(items_to_insert)

    def get_similar_question_sql(self, question: str, **kwargs) -> list:
        try:
            embedding = self._embed(question)
            
            # 调试信息
            if VERBOSE:
                print(f"\n===调试: 相似向量查询===")
                print(f"查询向量长度: {len(embedding)}")
            else:
                print(f"[INFO] 查询相似问题 (文本长度: {len(question)})")
            
            with self.conn.cursor() as cur:
                # 修改查询语法，使用正确的向量类型转换
                # 尝试将Python列表转换为PG向量格式
                embedding_str = f"[{','.join(str(x) for x in embedding)}]"
                query = f"""
                    SELECT content FROM {self.table_name}
                    WHERE type = 'question_sql'
                    ORDER BY embedding <-> '{embedding_str}'::vector LIMIT 5
                """
                if VERBOSE:
                    print(f"执行查询: {query[:100]}...")
                cur.execute(query)
                rows = cur.fetchall()
            
            # 将结果格式化为Vanna需要的格式
            results = []
            for row in rows:
                content = row[0]
                # 检查是否包含分隔符"::"
                if "::" in content:
                    question, sql = content.split("::", 1)
                    results.append({"question": question.strip(), "sql": sql.strip()})
                else:
                    # 如果没有分隔符，整个内容当作SQL
                    results.append({"question": "", "sql": content.strip()})
            
            return results
        except Exception as e:
            self.conn.rollback()
            print(f"[ERROR] 查询相似问题失败: {str(e)}")
            raise Exception(f"查询相似问题失败: {e}")

    def get_related_ddl(self, question: str, **kwargs) -> list:
        return []

    def get_related_documentation(self, question: str, **kwargs) -> list:
        return []

    def get_training_data(self, **kwargs) -> pd.DataFrame:
        try:
            return pd.read_sql(f"SELECT * FROM {self.table_name}", self.conn)
        except Exception as e:
            raise Exception(f"获取训练数据失败: {e}")

    def remove_training_data(self, id: str, **kwargs) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self.table_name} WHERE id = %s", (id,))
                self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"删除训练数据失败: {e}")
