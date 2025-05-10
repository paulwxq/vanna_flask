# vanna_trainer.py

from vanna.chromadb.chromadb_vector import ChromaDB_VectorStore
from vanna.qianwen import QianWenAI_Chat  # 适配通义千问 Qwen 模型
from vanna.chromadb import ChromaDB_VectorStore
from dashscope import Generation
import os

# 构造继承 Vanna 组合类
class QwenVanna(ChromaDB_VectorStore, QianWenAI_Chat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        QianWenAI_Chat.__init__(self, config=config)

vn = QwenVanna(config={'api_key': 'sk-db68e37f00974031935395315bfe07f0', 'model': 'qwen-max'})
vn.connect_to_postgres(
    host='127.0.0.1',
    dbname='works_dw',
    user='postgres',
    password='postgres',
    port=5432
)

# 训练函数：4种类型

def train_ddl(ddl_sql: str):
    print(f"[DDL] Training on DDL:\n{ddl_sql}")
    vn.train(ddl=ddl_sql)

def train_documentation(doc: str):
    print(f"[DOC] Training on documentation:\n{doc}")
    vn.train(documentation=doc)

def train_sql_example(sql: str):
    print(f"[SQL] Training on SQL:\n{sql}")
    vn.train(sql=sql)

def train_question_sql_pair(question: str, sql: str):
    print(f"[Q-S] Training on:\nQ: {question}\nSQL: {sql}")
    vn.train(question=question, sql=sql)
