# reset_pgvector.py
"""
用于重置pgvector数据库表的脚本
会删除现有的vanna_pgvector表并创建新表
"""

import sys
import os

# 添加父目录到路径，确保能正确导入项目模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vanna_pgvector_qwen import vn

def reset_pgvector_table():
    print("开始重置pgvector表...")
    if hasattr(vn, 'reset_table') and callable(getattr(vn, 'reset_table')):
        success = vn.reset_table()
        if success:
            print("✅ pgvector表重置成功，现在可以重新训练了")
        else:
            print("❌ pgvector表重置失败")
    else:
        print("❌ vn实例没有reset_table方法")

if __name__ == "__main__":
    reset_pgvector_table() 