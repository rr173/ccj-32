# Privacy Budget Service

差分隐私预算管理平台。三个**可独立部署**的微服务，纯 Python 标准库实现（零第三方依赖），SQLite 持久化，Docker Compose 一键部署。

## 架构

```
                ┌────────────────────┐
                │  registry-service  │  数据集登记：敏感度、周期预算、
                │     :8001          │  单次上限、组合规则（策略源头）
                └─────────▲──────────┘
                          │ 拉取策略(带缓存)
        ┌─────────────────┴──────────────────┐
        │                                    │
┌───────┴────────┐                  ┌────────┴────────┐
│ budget-service │◀──预占/提交/返还──│  authz-service  │
│     :8002      │                  │      :8003      │
│ 预算核算：账户、 │                  │ 查询授权：申请、 │
│ 组合累计、审计账 │                  │ 审批、撤销、执行、│
│ 本、周期切换     │                  │ 结果签名与验证   │
└────────────────┘                  └─────────────────┘
```

每个服务独占自己的数据库文件，通过 HTTP 交互，可分别扩容/重启/部署。

## 核心规则

**预算与组合**
- 预算按 `(dataset, subject, period)` 记账：`budget_total / consumed / reserved`。
- 组合规则为纯 DP（pure composition）：同一主体在同一周期内多次查询的 ε 直接累加进 `consumed`，累计不得超过周期预算。
- 每次申请消耗带**有效期**（`ttl_seconds`，且不超过周期结束时间）和**敏感度**（随数据集登记，决定拉普拉斯噪声尺度 Δf/ε）。

**并发原子预占**
- 所有余额变更在 `BEGIN IMMEDIATE` 事务内完成（写锁先行，串行化检查-扣减），并叠加条件更新
  `UPDATE accounts SET reserved=reserved+? WHERE consumed+reserved+?<=budget_total` 并校验影响行数。
- 两个并发请求不可能同时看到余量而一起超支；失败者记 `CONFLICT` 审计事件并返回 409。

**返还 / 保留规则（全部幂等，绝不重复返还）**

| 事件 | 规则 |
|---|---|
| 申请取消 | 预占未提交 → 返还（REFUND），重复取消返回同一结果 |
| 执行失败 | 自动返还，申请置 FAILED |
| 结果重试 | 生成 version+1 的新申请、新预占；原申请保持终态，不二次返还 |
| 已执行(committed) | 消耗**保留**，任何返还请求返回 409 |
| 预占 TTL 过期 | 自动 EXPIRE 并释放；之后到达的返还是幂等空操作 |
| 周期切换 | 旧周期关闭，未提交预占全部 EXPIRE（不结转）；新周期预算重置；消耗不跨周期迁移 |

状态机 `RESERVED → COMMITTED | REFUNDED | EXPIRED`，每次跃迁由
`UPDATE ... WHERE state='RESERVED'` + 行数校验保证恰好执行一次。

**结果绑定与撤销**
- 结果记录绑定 `application_id + app_version + 噪声参数(mechanism/ε/δ/敏感度)`，
  以 HMAC-SHA256 签名；`POST /results/verify` 离线验证来源与完整性，篡改 payload 即失效。
- 撤销授权：未执行的申请立即停止并返还预算；已执行的申请仅标记
  `authorization_revoked`，消耗保留，已生成结果仍可验证。

**管理员审计**
- `GET /admin/ledger`（budget-service）：每次 RESERVE / COMMIT / REFUND /
  EXPIRE / REJECT / CONFLICT / OPEN / ROLLOVER，含金额、原因和操作后余额快照。
- `GET /applications?status=...`（authz-service）：申请全生命周期。

## 快速开始

### Docker（推荐）

```bash
cd privacy-budget
docker compose up --build
```

### 本地（无需任何依赖，Python ≥ 3.9）

```bash
cd privacy-budget
PORT=8001 DB_PATH=/tmp/reg.db   python3 registry-service/app.py &
PORT=8002 DB_PATH=/tmp/bud.db   REGISTRY_URL=http://localhost:8001 \
  python3 budget-service/app.py &
PORT=8003 DB_PATH=/tmp/aut.db   REGISTRY_URL=http://localhost:8001 \
  BUDGET_URL=http://localhost:8002 AUTHZ_SECRET=change-me \
  python3 authz-service/app.py &
```

### 端到端测试（含并发、返还、周期切换、撤销验证）

```bash
python3 tests/test_flow.py
```

## 使用流程示例

```bash
# 1. 登记数据集（周期预算 ε=10/天，单次上限 5）
curl -X POST localhost:8001/datasets -d '{
  "name":"census","sensitivity":"high","epsilon_per_period":10,
  "period_seconds":86400,"max_epsilon_per_request":5,"composition":"pure"}'

# 2. 提交统计申请（自动原子预占预算）
curl -X POST localhost:8003/applications -d '{
  "dataset_id":"ds-xxx","subject_id":"alice","epsilon":2.0,
  "sensitivity":1.0,"query":{"sql":"SELECT count(*) FROM census"},
  "idempotency_key":"req-001"}'

# 3. 审批 → 执行（注入拉普拉斯噪声，消耗提交，结果签名）
curl -X POST localhost:8003/applications/app-xxx/approve
curl -X POST localhost:8003/applications/app-xxx/execute -d '{"true_value": 100}'

# 4. 验证结果来源（撤销授权后依然有效）
curl -X POST localhost:8003/results/verify -d '{"result_id":"res-xxx"}'

# 5. 管理员审计
curl 'localhost:8002/admin/ledger?dataset_id=ds-xxx&subject_id=alice'
```

## API 一览

**registry-service :8001**
| 方法 | 路径 | 说明 |
|---|---|---|
| POST | /datasets | 登记数据集与预算策略 |
| GET | /datasets, /datasets/{id} | 查询 |
| PUT | /datasets/{id} | 更新策略 |

**budget-service :8002**
| 方法 | 路径 | 说明 |
|---|---|---|
| POST | /reserve | 原子预占（幂等键 request_id） |
| POST | /commit | 提交消耗（幂等） |
| POST | /refund | 返还（幂等；已提交→409） |
| POST | /sweep | 主动过期扫描（默认惰性过期） |
| GET | /accounts/{ds}/{subject} | 账户余额与组合累计 |
| GET | /reservations/{request_id} | 预占状态 |
| GET | /admin/ledger | 审计账本 |

**authz-service :8003**
| 方法 | 路径 | 说明 |
|---|---|---|
| POST | /applications | 提交申请（幂等键 idempotency_key） |
| POST | /applications/{id}/approve | 审批 |
| POST | /applications/{id}/execute | 执行（加噪、提交消耗、签发结果） |
| POST | /applications/{id}/cancel | 取消（返还） |
| POST | /applications/{id}/revoke | 撤销授权 |
| POST | /applications/{id}/retry | 重试（version+1，新预占） |
| GET | /applications[/{id}] | 查询（惰性同步过期状态） |
| GET | /results/{id} | 取结果 |
| POST | /results/verify | 验证结果来源 |

## 环境变量

| 变量 | 服务 | 默认 | 说明 |
|---|---|---|---|
| `PORT` | 全部 | 8001/8002/8003 | 监听端口 |
| `DB_PATH` | 全部 | /data/*.db | SQLite 路径 |
| `REGISTRY_URL` | budget, authz | http://localhost:8001 | |
| `BUDGET_URL` | authz | http://localhost:8002 | |
| `AUTHZ_SECRET` | authz | dev-secret-change-me | 结果签名密钥（生产必须改） |
| `RESERVATION_TTL_SECONDS` | authz | 300 | 预占有效期 |
| `POLICY_CACHE_TTL` | budget | 30 | 数据集策略缓存秒数 |

## 说明与扩展方向

- 执行器当前内置拉普拉斯机制演示（`true_value + Lap(Δf/ε)`），生产环境可将
  `execute` 替换为对接真实查询引擎，预算/授权/审计逻辑不变。
- SQLite 的写锁已保证单库原子性；如需多副本 budget-service，可将
  `common/db.py` 切换为 PostgreSQL（`SELECT ... FOR UPDATE` / 条件更新语义一致）。
- 组合规则当前为 pure DP；账本已记录每次消耗，可扩展 RDP/zCDP 会计器而无需改动接口。
