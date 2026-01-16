# 医美回访系统 - 数据导入指南

## 目录结构

```
data/import/
├── initial/                     # 【初始数据】全量导入，用于新数据
│   ├── common/                  # 集团公共数据（所有机构共享）
│   │   ├── institutions.json    # 机构信息
│   │   ├── doctors.json         # 医生信息
│   │   ├── projects.json        # 医美项目
│   │   ├── products.json        # 医美产品
│   │   └── medical_relations.json  # 项目/产品关联关系
│   │
│   └── institutions/            # 各机构独立数据
│       ├── BJ-HA-001/           # 机构代码作为目录名
│       │   ├── customers.json   # 该机构的客户
│       │   └── consumption_records.json  # 该机构的消费记录
│       └── SH-ML-002/
│           └── ...
│
├── incremental/                 # 【增量数据】用于更新/修改已有数据
│   ├── pending/                 # 待处理（按日期分目录）
│   │   └── 2026-01-15/
│   │       ├── customers_add.json      # 新增客户
│   │       ├── customers_update.json   # 更新客户
│   │       └── consumption_records.json # 新消费记录
│   ├── processed/               # 已处理（自动归档）
│   └── failed/                  # 处理失败（需人工处理）
│
└── templates/                   # 数据模板（参考格式）
```

---

## 一、全新数据导入（首次/新增机构）

### 适用场景
- 系统首次部署
- 新增一家医美机构
- 导入全新的项目/产品

### 操作步骤

#### 1. 准备数据文件

将 JSON 数据文件放入对应目录：

| 数据类型 | 存放位置 |
|---------|---------|
| 机构信息 | `data/import/initial/common/institutions.json` |
| 医生信息 | `data/import/initial/common/doctors.json` |
| 项目信息 | `data/import/initial/common/projects.json` |
| 产品信息 | `data/import/initial/common/products.json` |
| 客户信息 | `data/import/initial/institutions/{机构代码}/customers.json` |
| 消费记录 | `data/import/initial/institutions/{机构代码}/consumption_records.json` |

#### 2. 数据格式示例

**机构 (institutions.json)**
```json
[
  {
    "institution_code": "BJ-HA-001",
    "name": "北京华美医疗美容医院",
    "alias": "华美北京",
    "type": "HOSPITAL",
    "status": "ACTIVE"
  }
]
```

**客户 (customers.json)**
```json
[
  {
    "customer_code": "BJ-HA-001-C0001",
    "person": {
      "name": "张小红",
      "phone": "13811110001",
      "gender": "FEMALE",
      "birthday": "1990-01-16"
    },
    "vip_level": "GOLD",
    "source": "REFERRAL",
    "consultant_id": null,
    "notes": "VIP客户，注意服务质量"
  }
]
```

**消费记录 (consumption_records.json)**
```json
[
  {
    "order_number": "BJ-HA-001-ORD-20260110-0001",
    "customer_code": "BJ-HA-001-C0001",
    "doctor_code": "DOC-BJ-001",
    "order_date": "2026-01-10",
    "order_type": "PROJECT",
    "project_code": "BOTOX-001",
    "product_code": "BOTOX-100U",
    "total_amount": 3980.00,
    "discount_amount": 398.00,
    "actual_amount": 3582.00,
    "payment_method": "WECHAT",
    "payment_status": "PAID"
  }
]
```

#### 3. 执行导入

```bash
# 进入项目目录
cd D:\pyspace\Revisit

# 执行全量导入
python -m scripts.import_data initial
```

#### 4. 查看导入结果

导入完成后会显示统计信息：
```
==================================================
导入统计:
==================================================
  ✅ institutions: 2
  ✅ doctors: 6
  ✅ projects: 8
  ✅ products: 8
  ✅ customers: 5
  ✅ consumption_records: 5
  ✅ relations: 7
  ✅ errors: 0
==================================================
```

---

## 二、更新/修改已有数据（增量更新）

### 适用场景
- 修改客户信息（电话、地址等）
- 添加新的消费记录
- 更新客户 VIP 等级
- 日常数据同步

### 操作步骤

#### 1. 创建日期目录

```bash
# 创建今天的增量目录
mkdir data\import\incremental\pending\2026-01-15
```

#### 2. 准备增量数据文件

**新增数据 (customers_add.json)**
```json
{
  "operation": "INSERT",
  "timestamp": "2026-01-15T10:30:00",
  "institution_code": "BJ-HA-001",
  "data": [
    {
      "customer_code": "BJ-HA-001-C0006",
      "person": {
        "name": "新客户",
        "phone": "13900001111",
        "gender": "FEMALE",
        "birthday": "1995-05-20"
      },
      "vip_level": "NORMAL",
      "source": "ONLINE"
    }
  ]
}
```

**更新数据 (customers_update.json)**
```json
{
  "operation": "UPDATE",
  "timestamp": "2026-01-15T10:30:00",
  "institution_code": "BJ-HA-001",
  "data": [
    {
      "customer_code": "BJ-HA-001-C0001",
      "updates": {
        "vip_level": "PLATINUM",
        "notes": "升级为铂金会员"
      }
    },
    {
      "customer_code": "BJ-HA-001-C0002",
      "updates": {
        "person": {
          "phone": "13899998888"
        }
      }
    }
  ]
}
```

**新消费记录 (consumption_records.json)**
```json
{
  "operation": "INSERT",
  "timestamp": "2026-01-15T10:30:00",
  "institution_code": "BJ-HA-001",
  "data": [
    {
      "order_number": "BJ-HA-001-ORD-20260115-0001",
      "customer_code": "BJ-HA-001-C0001",
      "doctor_code": "DOC-BJ-001",
      "order_date": "2026-01-15",
      "order_type": "PROJECT",
      "project_code": "FILLER-001",
      "product_code": "JUVEDERM-VOLUMA",
      "total_amount": 8800.00,
      "discount_amount": 880.00,
      "actual_amount": 7920.00,
      "payment_method": "ALIPAY",
      "payment_status": "PAID"
    }
  ]
}
```

#### 3. 执行增量导入

```bash
# 处理所有待处理的增量数据
python -m scripts.import_data incremental

# 或指定日期
python -m scripts.import_data incremental --date 2026-01-15
```

#### 4. 处理结果

- **成功**：文件自动移至 `processed/` 目录
- **失败**：文件移至 `failed/` 目录，需人工检查

---

## 三、数据模板

在 `data/import/templates/` 目录下有各类数据的模板文件，可参考格式：

```bash
# 查看客户数据模板
type data\import\templates\customer.template.json
```

---

## 四、常见问题

### Q1: 导入时提示"外键约束失败"
**原因**：导入顺序不对，或关联数据不存在
**解决**：确保按以下顺序导入：
1. 机构 → 2. 医生 → 3. 项目 → 4. 产品 → 5. 客户 → 6. 消费记录

### Q2: 更新客户信息后没生效
**原因**：可能使用了错误的 customer_code
**解决**：检查 customer_code 是否正确，格式为 `{机构代码}-C{序号}`

### Q3: 如何删除数据？
**方式**：创建删除文件 `xxx_delete.json`
```json
{
  "operation": "DELETE",
  "timestamp": "2026-01-15T10:30:00",
  "institution_code": "BJ-HA-001",
  "data": [
    { "customer_code": "BJ-HA-001-C0099" }
  ]
}
```

### Q4: 如何批量更新大量数据？
**建议**：
1. 将数据分批，每批不超过 1000 条
2. 使用增量更新，而不是全量重导
3. 在业务低峰期执行

---

## 五、数据同步流程

```
JSON 文件
    ↓
PostgreSQL (主数据库)
    ↓ 自动同步
    ├─→ NebulaGraph (图关系)
    ├─→ Qdrant (向量搜索)
    └─→ ClickHouse (数据分析)
```

导入 PostgreSQL 后，系统会自动将数据同步到其他数据库。

---

## 六、快速参考

| 操作 | 命令 |
|-----|------|
| 全量导入 | `python -m scripts.import_data initial` |
| 增量导入（全部） | `python -m scripts.import_data incremental` |
| 增量导入（指定日期） | `python -m scripts.import_data incremental --date 2026-01-15` |
| 查看帮助 | `python -m scripts.import_data --help` |

