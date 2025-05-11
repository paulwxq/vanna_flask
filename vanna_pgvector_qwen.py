from pgvector_store import PgVectorStore
from vanna.qianwen import QianWenAI_Chat
from typing import List
# from dashscope import TextEmbedding  # 注释掉阿里云API
import json, os, requests  # 添加requests用于调用Ollama API
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 读取日志级别配置
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
VERBOSE = LOG_LEVEL.upper() in ['DEBUG', 'TRACE']


class VannaPgVectorQwen(QianWenAI_Chat,PgVectorStore, ):
    """
    Vanna 引擎实例：结合 Qwen 模型 + pgvector 向量存储
    """
    def __init__(self, config=None):
        QianWenAI_Chat.__init__(self, config=config)
        PgVectorStore.__init__(self, config=config)
        # 设置Ollama API地址和模型
        self.ollama_base_url = config.get('ollama_base_url', 'http://localhost:11434')
        self.embedding_model_name = config.get('ollama_embedding_model', 'bge-m3:latest')
        print(f"正在使用Ollama作为embedding模型({self.embedding_model_name})")
        
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
        使用本地Ollama生成文本向量，替代阿里云的embedding API
        """
        if VERBOSE:
            print(f"\n===调试: 开始生成嵌入向量===")
            print(f"输入文本长度: {len(data)} 字符")
        else:
            print(f"[INFO] 使用Ollama ({self.embedding_model_name}) 生成嵌入向量 (文本长度: {len(data)})")
        
        # 处理空字符串输入
        if not data or len(data.strip()) == 0:
            print("[WARNING] 输入文本为空，返回零向量")
            # 返回1024维的零向量
            return [0.0] * 1024
        
        try:
            # 直接调用Ollama API
            response = requests.post(
                f"{self.ollama_base_url}/api/embeddings",
                json={"model": self.embedding_model_name, "prompt": data}
            )
            
            if response.status_code != 200:
                error_msg = f"API请求错误: {response.status_code}, {response.text}"
                if VERBOSE:
                    print(f"错误: {error_msg}")
                else:
                    print(f"[ERROR] {error_msg}")
                raise Exception(error_msg)
                
            result = response.json()
            vector = result.get("embedding")
            
            if not vector:
                error_msg = "API返回中没有embedding字段"
                if VERBOSE:
                    print(f"错误: {error_msg}")
                else:
                    print(f"[ERROR] {error_msg}")
                raise Exception(error_msg)
                
            if VERBOSE:
                print(f"向量长度: {len(vector)}")
                print(f"向量前5个元素: {vector[:5]}")
                print("===调试: 嵌入向量生成成功===\n")
            else:
                print(f"[INFO] 成功生成向量，维度: {len(vector)}")
                
            return vector
            
        except Exception as e:
            print(f"[ERROR] Ollama嵌入向量生成异常: {str(e)}")
            if VERBOSE:
                print("===调试: 嵌入向量生成失败===\n")
            raise


# 默认配置（建议你可后续用 .env 或 config.py 替换）
# 从环境变量读取配置
config = {
    'api_key': os.environ.get('QWEN_API_KEY'),
    'model': os.environ.get('QWEN_MODEL', 'qwen-plus'),
    # 'embedding_model': os.environ.get('QWEN_EMBEDDING_MODEL', 'text-embedding-v2'),  # 注释掉阿里云embedding模型
    'ollama_base_url': os.environ.get('OLLAMA_BASE_URL', 'http://localhost:11434'),  # 添加Ollama配置
    'ollama_embedding_model': os.environ.get('OLLAMA_EMBEDDING_MODEL', 'bge-m3:latest'),  # 添加Ollama模型配置
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
