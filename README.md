# vanna-flask

# 创建向量数据库
create database pgvector_store;
CREATE EXTENSION vector;

CREATE TABLE vanna_pgvector (
    id SERIAL PRIMARY KEY,
    type TEXT,
    content TEXT,
    embedding VECTOR(768)
);
