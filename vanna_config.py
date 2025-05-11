from vanna.chromadb.chromadb_vector import ChromaDB_VectorStore
from vanna.qianwen import QianWenAI_Chat  # 适配通义千问 Qwen 模型
from vanna.chromadb import ChromaDB_VectorStore

# 构造继承 Vanna 组合类
class VannaInstance(ChromaDB_VectorStore, QianWenAI_Chat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        QianWenAI_Chat.__init__(self, config=config)

# 创建实例
vn = VannaInstance(config={'api_key': 'sk-db68e37f00974031935395315bfe07f0', 'model': 'qwen-max'})

# 配置数据库连接
def init_db_connection():
    vn.connect_to_postgres(
        host='127.0.0.1',
        dbname='works_dw',
        user='postgres',
        password='postgres',
        port=5432
    ) 