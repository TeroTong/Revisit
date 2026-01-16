# 数据导入目录结构

本目录用于存放待导入到 PostgreSQL 的 JSON 数据文件。

## 目录结构

```
data/import/
├── README.md                    # 本说明文件
├── initial/                     # 初始化数据（全量导入）
│   ├── common/                  # 集团公共数据
│   │   ├── institutions.json    # 机构数据
│   │   ├── projects.json        # 项目数据
│   │   ├── products.json        # 产品数据
│   │   ├── doctors.json         # 医生数据
│   │   └── medical_relations.json  # 医美关系数据
│   │
│   └── institutions/            # 机构特定数据
│       ├── BJ-HA-001/           # 北京华美机构
│       │   ├── customers.json   # 客户数据
│       │   ├── consumption_records.json  # 消费记录
│       │   ├── consultations.json        # 咨询记录
│       │   └── chat_sessions.json        # 聊天会话
│       │
│       └── SH-ML-002/           # 上海美莱机构
│           ├── customers.json
│           ├── consumption_records.json
│           ├── consultations.json
│           └── chat_sessions.json
│
├── incremental/                 # 增量更新数据
│   ├── pending/                 # 待处理的增量数据
│   │   ├── 2026-01-14/          # 按日期分目录
│   │   │   ├── customers_add.json
│   │   │   ├── customers_update.json
│   │   │   ├── consumption_records.json
│   │   │   └── ...
│   │   └── ...
│   │
│   ├── processed/               # 已处理的增量数据（归档）
│   │   └── 2026-01-13/
│   │       └── ...
│   │
│   └── failed/                  # 处理失败的数据（需人工处理）
│       └── ...
│
└── templates/                   # JSON 数据模板
    ├── institution.template.json
    ├── project.template.json
    ├── product.template.json
    ├── doctor.template.json
    ├── customer.template.json
    ├── consumption_record.template.json
    └── ...
```

## 数据格式说明

### 1. 初始化数据 (initial/)

用于首次全量导入，文件内容为 JSON 数组格式：

```json
[
  { "field1": "value1", "field2": "value2", ... },
  { "field1": "value1", "field2": "value2", ... }
]
```

### 2. 增量数据 (incremental/)

#### 新增数据 (*_add.json)
```json
{
  "operation": "INSERT",
  "timestamp": "2026-01-14T10:30:00",
  "data": [
    { "field1": "value1", ... }
  ]
}
```

#### 更新数据 (*_update.json)
```json
{
  "operation": "UPDATE",
  "timestamp": "2026-01-14T10:30:00",
  "data": [
    { 
      "id": "xxx",  // 或其他唯一标识字段
      "updates": { "field1": "new_value1", ... }
    }
  ]
}
```

#### 删除数据 (*_delete.json)
```json
{
  "operation": "DELETE",
  "timestamp": "2026-01-14T10:30:00",
  "data": [
    { "id": "xxx" }
  ]
}
```

## 使用方式

### 初始导入
```bash
python scripts/import_data.py --initial
```

### 增量更新
```bash
python scripts/import_data.py --incremental
python scripts/import_data.py --incremental --date 2026-01-14
```

### 导入特定文件
```bash
python scripts/import_data.py --file data/import/initial/common/projects.json --table project
```

## 注意事项

1. **编码格式**: 所有 JSON 文件必须使用 UTF-8 编码
2. **日期格式**: 日期使用 ISO 8601 格式 (YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS)
3. **数组字段**: PostgreSQL 数组字段使用 JSON 数组表示，如 `["tag1", "tag2"]`
4. **空值处理**: 空值使用 `null`，不要使用空字符串
5. **机构编码**: 机构目录名必须与 `institution_code` 一致

