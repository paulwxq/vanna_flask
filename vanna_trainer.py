# vanna_trainer.py

# from vanna_config import vn, init_db_connection
from vanna_pgvector_qwen import vn, init_db_connection

# 初始化数据库连接
init_db_connection()

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
