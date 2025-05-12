# vanna-flask
# embedding 模型使用本地，LLM使用阿里云QWEN，向量数据库使用PgVector

# 创建向量数据库
create database pgvector_store;
CREATE EXTENSION vector;

CREATE TABLE vanna_pgvector (
    id SERIAL PRIMARY KEY,
    type TEXT,
    content TEXT,
    embedding VECTOR(768)
);

# Vanna Flask 应用

## 本地Ollama Embedding模型

项目现在**完全使用**本地Ollama的bge-m3模型来生成文本向量，同时继续使用阿里云的QWEN模型处理自然语言。这种混合模式有以下优势：

1. 降低API调用成本（仅LLM使用API，embedding全部在本地生成）
2. 提高向量生成速度 
3. 解决长文本分割问题
4. 移除长度限制

### 安装和使用

1. 确保本地已安装Ollama并运行：
   - 下载地址：https://ollama.com/download
   - 确保Ollama服务运行：`ollama serve`

2. 手动拉取BGE-M3模型（必须步骤）：
   ```
   ollama pull bge-m3:latest
   ```

3. 重置向量数据库并重新训练数据：
   ```
   python tools/retrain_with_ollama.py
   ```

4. 启动应用程序：
   ```
   python app.py
   ```

## 环境变量配置

在`.env`文件中配置以下变量：

```
# Qwen API配置
QWEN_API_KEY=your_api_key
QWEN_MODEL=qwen-plus

# Ollama配置
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_EMBEDDING_MODEL=bge-m3:latest  # 指定用于生成embedding的Ollama模型

# PgVector数据库配置 
PGVECTOR_HOST=127.0.0.1
PGVECTOR_PORT=5432
PGVECTOR_DB=pgvector_store
PGVECTOR_USER=postgres
PGVECTOR_PASSWORD=postgres
PGVECTOR_TABLE=vanna_pgvector

# 批处理配置
BATCH_PROCESSING_ENABLED=true
BATCH_SIZE=50
MAX_WORKERS=4
LOG_LEVEL=INFO
```
