# 医美客户回访系统 - 部署指南

## 系统要求

- Windows 10/11 或 Windows Server
- Python 3.10+
- Docker Desktop
- cpolar（用于内网穿透）

## 完整部署流程

### 第一步：启动 Docker 数据库服务

```powershell
cd D:\pyspace\Revisit
docker-compose up -d postgres metad storaged graphd clickhouse qdrant
```

等待所有容器启动完成（约30秒），检查状态：
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

所有服务应显示 `healthy` 或 `Up`。

### 第二步：初始化数据库

```powershell
cd D:\pyspace\Revisit
$env:HTTP_PROXY=''; $env:HTTPS_PROXY=''
python -m scripts.init_database
```

这将创建：
- PostgreSQL: 31 张表
- NebulaGraph: 5 种顶点类型，15 种边类型
- ClickHouse: 13 张分析表
- Qdrant: 3 个向量集合

### 第三步：导入数据

**导入初始数据：**
```powershell
python -m scripts.import_data initial
```

**导入增量数据（可选）：**
```powershell
python -m scripts.import_data incremental
```

### 第四步：验证数据同步

```powershell
python -m tests.check_data
```

确认所有数据库都有数据。

### 第五步：启动主服务

```powershell
$env:HTTP_PROXY=''; $env:HTTPS_PROXY=''; $env:no_proxy='*'
python main.py
```

服务启动后，本地访问地址：
- 前端界面：http://localhost:8000
- API 文档：http://localhost:8000/api/docs
- 健康检查：http://localhost:8000/health

### 第六步：配置外网访问（cpolar）

1. **启动 cpolar：**
   ```powershell
   cpolar http 8000
   ```

2. **获取外网地址：**
   cpolar 启动后会显示类似地址：
   ```
   https://xxxxxx.cpolar.top -> http://localhost:8000
   ```

3. **医美机构访问：**
   将 cpolar 提供的 HTTPS 地址分享给医美机构即可访问系统。

## 日常运维

### 启动服务（每日）

```powershell
# 1. 启动 Docker（如果未运行）
docker-compose up -d postgres metad storaged graphd clickhouse qdrant

# 2. 等待服务就绪
Start-Sleep -Seconds 30

# 3. 启动主服务
cd D:\pyspace\Revisit
$env:HTTP_PROXY=''; $env:HTTPS_PROXY=''; $env:no_proxy='*'
python main.py

# 4. 启动外网穿透（新窗口）
Start-Process cpolar -ArgumentList "http 8000"
```

### 停止服务

```powershell
# 停止 Python 服务
Get-Process python | Stop-Process -Force

# 停止 cpolar
Get-Process cpolar | Stop-Process -Force

# 停止 Docker 容器（可选）
docker-compose down
```

### 查看日志

```powershell
# 查看应用日志
Get-Content D:\pyspace\Revisit\logs\app.log -Tail 100
```

### 数据备份

```powershell
# PostgreSQL 备份
docker exec postgres pg_dump -U postgres revisit > backup_$(Get-Date -Format 'yyyyMMdd').sql
```

## 常见问题

### Q: 端口 8000 被占用
```powershell
Get-Process python | Stop-Process -Force
```

### Q: Qdrant 连接失败（502 Bad Gateway）
检查代理设置，确保禁用代理：
```powershell
$env:HTTP_PROXY=''; $env:HTTPS_PROXY=''; $env:no_proxy='*'
```

### Q: cpolar 外网地址无法访问
1. 确保主服务已启动（端口 8000 在监听）
2. 检查 cpolar 是否正常运行
3. 免费版 cpolar 地址会定期更换，需要重新获取

## 数据导入格式

数据文件放在 `data/import/` 目录下：
- `initial/` - 初始导入数据
- `incremental/` - 增量更新数据

参考 `data/import/templates/` 中的模板格式。

