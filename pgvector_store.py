import psycopg2
import pandas as pd
from typing import List
from vanna.base import VannaBase


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
                print(f"表 {self.table_name} 已重新创建，向量维度为1536")
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
                        embedding VECTOR(1536)
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
            print(f"\n===调试: 数据库插入===")
            print(f"数据类型: {data_type}")
            print(f"嵌入向量类型: {type(embedding)}")
            if isinstance(embedding, list):
                print(f"嵌入向量长度: {len(embedding)}")
                print(f"嵌入向量前5个元素: {embedding[:5]}")
            else:
                print(f"警告: 嵌入向量不是列表类型! 而是 {type(embedding)}")
            
            with self.conn.cursor() as cur:
                try:
                    cur.execute(
                        f"INSERT INTO {self.table_name} (type, content, embedding) VALUES (%s, %s, %s)",
                        (data_type, content, embedding)
                    )
                    print("SQL插入执行成功，准备提交...")
                    self.conn.commit()
                    print("事务提交成功")
                except Exception as e:
                    print(f"SQL执行或提交错误: {str(e)}")
                    self.conn.rollback()
                    print("事务已回滚")
                    raise
            return "ok"
        except Exception as e:
            self.conn.rollback()
            print(f"_insert方法最终错误: {str(e)}")
            raise Exception(f"插入数据失败: {e}")

    def add_ddl(self, ddl: str, **kwargs) -> str:
        return self._insert("ddl", ddl)

    def add_documentation(self, doc: str, **kwargs) -> str:
        return self._insert("documentation", doc)

    def add_question_sql(self, question: str, sql: str, **kwargs) -> str:
        return self._insert("question_sql", f"{question} :: {sql}")

    def get_similar_question_sql(self, question: str, **kwargs) -> list:
        try:
            embedding = self._embed(question)
            
            # 调试信息
            print(f"\n===调试: 相似向量查询===")
            print(f"查询向量长度: {len(embedding)}")
            
            with self.conn.cursor() as cur:
                # 修改查询语法，使用正确的向量类型转换
                # 尝试将Python列表转换为PG向量格式
                embedding_str = f"[{','.join(str(x) for x in embedding)}]"
                query = f"""
                    SELECT content FROM {self.table_name}
                    WHERE type = 'question_sql'
                    ORDER BY embedding <-> '{embedding_str}'::vector LIMIT 5
                """
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
            print(f"查询相似问题失败详情: {str(e)}")
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
