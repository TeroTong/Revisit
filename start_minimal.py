#!/usr/bin/env python3
"""
极简版启动脚本 - 不初始化任何数据库连接
"""
import os
os.environ['PYTHONIOENCODING'] = 'utf-8'
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# 创建 FastAPI 应用
app = FastAPI(title="医美客户回访系统")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 挂载 API
from api.main import app as api_app
app.mount("/api", api_app)

@app.get("/")
async def root():
    index_file = Path(__file__).parent / "static" / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"message": "医美回访系统"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

# 静态文件
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

if __name__ == "__main__":
    import uvicorn
    print("启动服务: http://localhost:8080")
    uvicorn.run(app, host="0.0.0.0", port=8080)

