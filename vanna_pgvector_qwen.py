from pgvector_store import PgVectorStore
from vanna.qianwen import QianWenAI_Chat
from typing import List
from dashscope import TextEmbedding
import json,os


class VannaPgVectorQwen(QianWenAI_Chat,PgVectorStore, ):
    """
    Vanna 引擎实例：结合 Qwen 模型 + pgvector 向量存储
    """
    def __init__(self, config=None):
        QianWenAI_Chat.__init__(self, config=config)
        PgVectorStore.__init__(self, config=config)
        
    def submit_prompt(self, prompt, **kwargs) -> str:
        """
        重写submit_prompt方法，为支持新版千问模型
        """
        if prompt is None:
            raise Exception("Prompt is None")

        if len(prompt) == 0:
            raise Exception("Prompt is empty")

        # Count the number of tokens in the message log
        # Use 4 as an approximation for the number of characters per token
        num_tokens = 0
        for message in prompt:
            num_tokens += len(message["content"]) / 4

        if kwargs.get("model", None) is not None:
            model = kwargs.get("model", None)
            print(f"Using model {model} for {num_tokens} tokens (approx)")
            response = self.client.chat.completions.create(
                model=model,
                messages=prompt,
                stop=None,
                temperature=self.temperature,
                stream=True,  # 启用流式模式
            )
        elif kwargs.get("engine", None) is not None:
            engine = kwargs.get("engine", None)
            print(f"Using model {engine} for {num_tokens} tokens (approx)")
            response = self.client.chat.completions.create(
                engine=engine,
                messages=prompt,
                stop=None,
                temperature=self.temperature,
                stream=True,  # 启用流式模式
            )
        elif self.config is not None and "engine" in self.config:
            print(f"Using engine {self.config['engine']} for {num_tokens} tokens (approx)")
            response = self.client.chat.completions.create(
                engine=self.config["engine"],
                messages=prompt,
                stop=None,
                temperature=self.temperature,
                stream=True,  # 启用流式模式
            )
        elif self.config is not None and "model" in self.config:
            print(f"Using model {self.config['model']} for {num_tokens} tokens (approx)")
            response = self.client.chat.completions.create(
                model=self.config["model"],
                messages=prompt,
                stop=None,
                temperature=self.temperature,
                stream=True,  # 启用流式模式
            )
        else:
            if num_tokens > 3500:
                model = "qwen-long"
            else:
                model = "qwen-plus"

            print(f"Using model {model} for {num_tokens} tokens (approx)")
            response = self.client.chat.completions.create(
                model=model,
                messages=prompt,
                stop=None,
                temperature=self.temperature,
                stream=True,  # 启用流式模式
            )

        # 处理流式响应
        full_content = ""
        try:
            for chunk in response:
                if hasattr(chunk, 'choices') and len(chunk.choices) > 0:
                    delta = chunk.choices[0].delta
                    if hasattr(delta, 'content') and delta.content is not None:
                        full_content += delta.content
        except Exception as e:
            print(f"处理流式响应时出错: {e}")
            
        return full_content
        
    def generate_embedding(self, data: str) -> List[float]:
        """
        使用阿里云的embedding模型生成文本向量
        """
        print(f"\n===调试: 开始生成嵌入向量===")
        print(f"输入文本长度: {len(data)} 字符")
        
        try:
            # 从配置中读取嵌入模型名称，如果未指定则使用默认值
            embedding_model = self.config.get('embedding_model', 'text-embedding-v2')
            print(f"使用嵌入模型: {embedding_model}")
            
            resp = TextEmbedding.call(
                model=embedding_model,
                input=data,
                api_key=self.config.get('api_key')  # 使用与QianWenAI_Chat相同的API key
            )
            
            print(f"API状态码: {resp.status_code}")
            
            if resp.status_code == 200:
                # 打印输出结构
                print(f"输出结构: {list(resp.output.keys())}")
                print(f"embeddings类型: {type(resp.output.get('embeddings', []))}")
                
                if 'embeddings' in resp.output and len(resp.output['embeddings']) > 0:
                    embedding_data = resp.output['embeddings'][0]
                    print(f"嵌入向量数据类型: {type(embedding_data)}")
                    
                    # 打印一小部分嵌入向量数据
                    if isinstance(embedding_data, dict):
                        print(f"嵌入向量键: {list(embedding_data.keys())}")
                        if 'embedding' in embedding_data:
                            vector = embedding_data['embedding']
                            print(f"向量类型: {type(vector)}, 向量长度: {len(vector)}")
                            print(f"向量前5个元素: {vector[:5]}")
                            return vector
                        else:
                            print(f"错误: 嵌入向量字典中没有'embedding'键")
                            print(f"完整字典: {json.dumps(embedding_data)[:200]}...")
                            raise Exception(f"无效的嵌入格式: 字典中没有'embedding'键")
                    
                    elif isinstance(embedding_data, list):
                        print(f"向量长度: {len(embedding_data)}")
                        print(f"向量前5个元素: {embedding_data[:5]}")
                        return embedding_data
                    
                    else:
                        print(f"错误: 嵌入向量数据类型意外: {type(embedding_data)}")
                        print(f"数据示例: {str(embedding_data)[:200]}...")
                        raise Exception(f"无效的嵌入格式: {type(embedding_data)}")
                else:
                    print(f"错误: API响应中没有embeddings字段或为空")
                    print(f"完整响应: {json.dumps(resp.output)[:200]}...")
                    raise Exception("API响应中没有embeddings字段或为空")
            else:
                print(f"错误: API请求失败, 状态码: {resp.status_code}")
                print(f"错误信息: {resp.message}")
                raise Exception(f"生成嵌入失败: {resp.message}")
        
        except Exception as e:
            print(f"异常: {str(e)}")
            print("===调试: 嵌入向量生成失败===\n")
            raise
            
        print("===调试: 嵌入向量生成成功===\n")
        


# 默认配置（建议你可后续用 .env 或 config.py 替换）
  # 从环境变量读取配置
config = {
    'api_key': os.environ.get('QWEN_API_KEY'),
    'model': os.environ.get('QWEN_MODEL', 'qwen-plus'),
    'embedding_model': os.environ.get('QWEN_EMBEDDING_MODEL', 'text-embedding-v2'),
    'pgvector_host': os.environ.get('PGVECTOR_HOST', '127.0.0.1'),
    'pgvector_port': int(os.environ.get('PGVECTOR_PORT', 5432)),
    'pgvector_db': os.environ.get('PGVECTOR_DB', 'pgvector_store'),
    'pgvector_user': os.environ.get('PGVECTOR_USER', 'postgres'),
    'pgvector_password': os.environ.get('PGVECTOR_PASSWORD', 'postgres'),
    'pgvector_table': os.environ.get('PGVECTOR_TABLE', 'vanna_pgvector')
}

# 创建实例
vn = VannaPgVectorQwen(config=config)


def init_db_connection():
    """
    初始化 SQL 查询用的 PostgreSQL 连接（用于 run_sql）
    """
    vn.connect_to_postgres(
        host=os.environ.get('DB_HOST', '127.0.0.1'),
        dbname=os.environ.get('DB_NAME', 'works_dw'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', 'postgres'),
        port=int(os.environ.get('DB_PORT', 5432))
    )
