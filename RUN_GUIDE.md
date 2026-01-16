# 医美客户回访系统 - 运行指南

## 快速启动

### 方法一：图形化启动器（推荐）

双击运行项目根目录下的 `launcher.py` 文件，或在 PyCharm 中右键运行它。

```
D:\pyspace\Revisit\launcher.py
```

启动后会显示一个图形界面，包含以下功能选项卡：

1. **🚀 快速操作** - 常用功能的一键入口
2. **🗄️ 数据库管理** - 初始化和管理数据库
3. **📥 数据导入** - 导入JSON数据到数据库
4. **🎂 生日回访** - 运行生日回访任务
5. **🌐 API服务** - 启动Web API服务

### 方法二：直接运行Python脚本

如果你习惯使用 PyCharm 的运行配置，可以直接运行以下脚本：

| 功能 | 脚本文件 |
|------|----------|
| 系统检查 | `scripts/check_system.py` |
| 初始化数据库 | `scripts/init_database.py` |
| 导入数据 | `scripts/import_data.py` |
| 运行回访 | `scripts/run_reminders.py` |
| 启动API | `main.py` |

---

## 完整操作流程

### 第一步：系统检查

**目的**：验证所有配置正确，数据库服务可连接。

**操作**：在启动器中点击「🔍 系统检查」按钮

**预期结果**：
```
✅ PostgreSQL: Connected
✅ NebulaGraph: Space: revisit
✅ ClickHouse: 版本号
✅ Qdrant: 集合数: 3
```

如果任何数据库显示 ❌，请检查：
1. Docker 服务是否启动（`docker-compose up -d`）
2. `config/settings.py` 中的数据库配置是否正确

---

### 第二步：初始化数据库

**目的**：创建数据库表结构、索引、图谱空间等。

**操作**：在启动器中点击「🔄 初始化数据库」按钮

**选项说明**：
- `跳过 XXX`：如果某个数据库已初始化，可跳过
- `插入示例数据`：仅开发环境使用，会插入测试数据
- `强制重建`：⚠️ 危险！会删除所有现有数据

**预期结果**：
```
✅ PostgreSQL初始化完成
✅ NebulaGraph初始化完成
✅ ClickHouse初始化完成
✅ Qdrant向量数据库初始化完成
```

---

### 第三步：导入数据

**目的**：将机构、客户、消费记录等数据导入到数据库。

#### 数据文件结构

```
data/
└── import/
    ├── initial/                    # 初始全量数据
    │   ├── common/                 # 公共数据
    │   │   ├── institutions.json   # 机构列表
    │   │   ├── doctors.json        # 医生列表
    │   │   ├── projects.json       # 项目列表
    │   │   └── products.json       # 产品列表
    │   └── institutions/           # 各机构业务数据
    │       ├── BJ-HA-001/          # 机构代码
    │       │   ├── customers.json      # 客户数据
    │       │   └── consumption_records.json  # 消费记录
    │       └── SH-ML-002/
    │           └── ...
    │
    └── incremental/                # 增量数据
        └── pending/                # 待处理的增量文件
            └── 2026-01-15/         # 按日期组织
                ├── customers_add.json
                └── customers_update.json
```

#### JSON数据格式示例

**机构 (institutions.json)**:
```json
[
  {
    "institution_code": "BJ-HA-001",
    "name": "北京XX医美诊所",
    "type": "诊所",
    "address": "北京市朝阳区...",
    "phone": "010-12345678",
    "email": "contact@example.com",
    "status": "active"
  }
]
```

**客户 (customers.json)**:
```json
[
  {
    "customer_code": "BJ-HA-001-C0001",
    "person": {
      "name": "张三",
      "gender": "女",
      "birthday": "1990-05-15",
      "phone": "13800138000"
    },
    "vip_level": "金卡",
    "source": "朋友推荐",
    "tags": ["敏感肌", "抗衰老"],
    "notes": "皮肤敏感，注意产品选择"
  }
]
```

**消费记录 (consumption_records.json)**:
```json
[
  {
    "order_number": "BJ-HA-001-ORD-20260115-0001",
    "customer_code": "BJ-HA-001-C0001",
    "order_date": "2026-01-15",
    "items": [
      {
        "item_type": "project",
        "item_code": "PRJ-001",
        "item_name": "水光针",
        "quantity": 1,
        "unit_price": 2800.00,
        "discount": 0.9,
        "actual_price": 2520.00
      }
    ],
    "total_amount": 2520.00,
    "payment_method": "微信支付",
    "status": "completed"
  }
]
```

#### 执行导入

**操作**：在启动器中：
1. 切换到「📥 数据导入」选项卡
2. 选择「初始全量导入」或「增量数据导入」
3. 点击「开始导入」

**数据流向**：
```
JSON文件 → PostgreSQL(主数据) → NebulaGraph(图关系)
                              → Qdrant(向量搜索)
                              → ClickHouse(分析统计)
```

---

### 第四步：运行生日回访

**目的**：查找即将生日的客户，生成个性化回访话术。

**操作**：在启动器中：
1. 切换到「🎂 生日回访」选项卡
2. 选择要处理的机构（或选择"所有机构"）
3. 勾选「测试模式」（建议首次运行时使用）
4. 点击「运行生日回访」

**处理流程**：
```
1. 查询即将生日的客户（默认提前7天）
2. 获取每个客户的消费历史、咨询记录
3. 调用 LLM 生成个性化回访话术
4. 发送回访消息（测试模式下仅显示，不发送）
5. 记录回访结果
```

---

### 第五步（可选）：启动API服务

**目的**：提供RESTful API接口，支持前端应用调用。

**操作**：在启动器中点击「🌐 启动API服务」

**访问地址**：
- API文档：http://localhost:8000/docs
- 健康检查：http://localhost:8000/health

**主要API端点**：
| 端点 | 描述 |
|------|------|
| `GET /api/v1/customers/{code}` | 获取机构客户列表 |
| `GET /api/v1/reminders/{code}/upcoming-birthdays` | 即将生日客户 |
| `POST /api/v1/reminders/{code}/run` | 触发回访任务 |
| `GET /api/v1/analytics/dashboard` | 数据分析仪表板 |

---

## 定时任务

系统内置定时任务调度器，启动API服务后自动运行：

| 任务 | 时间 | 描述 |
|------|------|------|
| 每日回访 | 每天 09:00 | 自动运行生日回访任务 |
| 增量导入检查 | 每小时 | 检查并处理增量数据 |
| 健康检查 | 每30分钟 | 检查系统状态 |
| 日报生成 | 每天 08:00 | 生成每日统计报告 |

---

## 配置说明

主要配置在 `config/settings.py` 中：

```python
# 应用配置
APP_NAME = "医美客户回访系统"
ENVIRONMENT = "development"  # development / production
INSTITUTIONS = ["BJ-HA-001", "SH-ML-002"]  # 机构列表
BIRTHDAY_REMINDER_DAYS_AHEAD = 7  # 生日提醒提前天数

# 数据库配置
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "revisit"

NEBULA_HOST = "localhost"
NEBULA_PORT = 9669
NEBULA_SPACE = "revisit"

QDRANT_HOST = "localhost"
QDRANT_PORT = 6333

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "revisit"

# LLM配置
LLM_MODEL = "deepseek-chat"
LLM_API_URL = "https://api.deepseek.com/v1/chat/completions"
LLM_API_KEY = "your-api-key"  # 需要配置
```

---

## 常见问题

### Q: 数据库连接失败？

检查 Docker 服务：
```bash
docker-compose ps
docker-compose up -d
```

### Q: Qdrant 显示 502 Bad Gateway？

可能是代理配置问题，设置环境变量：
```
NO_PROXY=localhost,127.0.0.1
```

### Q: LLM 调用失败？

检查 `config/settings.py` 中的 LLM_API_KEY 是否正确配置。

### Q: 消费记录导入报错 "relation does not exist"？

需要先初始化数据库创建分区表，然后再导入数据。系统会自动根据机构代码创建对应的分区表。

---

## 项目结构

```
Revisit/
├── launcher.py          # 🚀 图形化启动器（推荐入口）
├── main.py              # FastAPI主应用
├── config/              # 配置文件
├── api/                 # API路由
├── services/            # 业务服务
│   ├── birthday_reminder.py   # 生日回访服务
│   ├── data_sync.py           # 数据同步服务
│   ├── llm_service.py         # LLM调用服务
│   └── notification_service.py # 通知服务
├── database/            # 数据库连接和操作
│   ├── postgres/        # PostgreSQL
│   ├── nebula/          # NebulaGraph
│   ├── clickhouse/      # ClickHouse
│   └── qdrant/          # Qdrant
├── scripts/             # 脚本文件
│   ├── init_database.py      # 初始化数据库
│   ├── import_data.py        # 导入数据
│   ├── check_system.py       # 系统检查
│   └── run_reminders.py      # 运行回访
├── tasks/               # 定时任务
├── data/                # 数据目录
│   └── import/          # 导入数据
├── logs/                # 日志目录
└── tests/               # 测试文件
```

