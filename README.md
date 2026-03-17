# Multi-Agent Ecommerce Lab

一个面向电商运营场景的多 agent 情报与决策 MVP。

当前公开版本聚焦一个**跨境内容电商运营研究场景**。项目目标不是做一个“看起来很聪明”的 demo，而是逐步搭出一个能够接入真实世界信号、进行可信度分层、时效性判断、角色分工和动作建议的运营中枢。

## 当前能力

### 1) D 线情报中枢骨架
- 领域数据模型：`weather / news / policy / platform_rule / competitor / trend_signal`
- 可信度分层：`formal_conclusions / hint_layer / observation_layer`
- 时效性判断：支持 freshness rule、过期降权、背景保留
- 角色分工：
  - `market_radar`
  - `competitor_watch`
  - `trend_scout`
  - `external_environment`
  - `daily_brief_synth`

### 2) 半真实运行（只读）
当前已经不是纯 mock：
- `weather`：接入 `wttr.in` 真实天气只读源
- `news`：接入 Google News RSS 公开新闻只读源
- `rule`：已接入公开规则页抓取链路，但是否 live 以运行时 `fetch_status` 为准
- `competitor_search`：已实现 TikTok Shop 搜索结果页最小 probe 闭环，live 成功时产出结构化竞品事件，受限时诚实降级为 degraded probe

### 3) 结果层可观测性
结果结构中会明确区分：
- `source_mode`: `live / mock / degraded`
- `fetch_status`
- `source_summary`
- `scan_agents`
- `freshness_summary`
- `layer_summary`

也就是说，系统不会把“解析器已存在”误报成“真实源已稳定打通”。

## 为什么做这个项目
电商运营里最难的不是“写出很多自动化动作”，而是先回答清楚这几个问题：
- 这条信息是否可信？
- 这条信息是否还新鲜？
- 这条信息属于哪个角色负责？
- 这条信息应该进入正式结论、提示层，还是只留在观察层？
- 这条信息是否足以驱动动作，还是应该先进入人工审批？

这个项目的核心就是把这些判断逻辑落到一个可运行、可测试、可持续接入真实源的 MVP 上。

## 项目结构

```text
multi-agent-ecommerce-lab/
├─ mock/                  # 业务样本数据
├─ scripts/               # 启动脚本 / 审批脚本
├─ tests/                 # TDD 测试
├─ tools/
│  └─ ops_mvp/            # 核心服务、模型、引擎、抓取器
└─ docs/                  # 公开版路线图与说明
```

## 快速开始

### 环境
- Python 3.12+
- 当前实现不依赖额外第三方 Python 包（标准库为主）

### 运行测试
```bash
python -m pytest tests/test_ops_mvp.py -q
```

### 启动本地服务
```bash
python -m tools.ops_mvp
```

或：
```bash
python scripts/run_ops_mvp.py
```

启动后访问：
- `http://127.0.0.1:8765/`
- `http://127.0.0.1:8765/api/state`

### 手动审批动作
```bash
python scripts/approve_ops_action.py <action_id> [approved|rejected] [reason]
```

## 当前边界
这个公开仓库是一个**可公开版子集**，只保留多 agent 电商主线相关代码与测试，不包含私人工作区中的其他工具、日志、记忆文件和设备相关内容。

同时，当前阶段也有明确边界：
- 不宣称已经打通所有 TikTok Shop 页面
- 不宣称已拥有后台级别经营数据
- 暂未接入登录态接口、自动执行链路和复杂 UI
- 某些真实源会因页面结构变化而退回 degraded/fallback，这是设计内行为，不会假装完全 live

## 下一步方向
- 补稳 `competitor_watch` 的商品详情页只读源
- 再补第二个规则/政策备用源
- 继续扩展真实只读信号，减少 mock 占比
- 在保持诚实口径的前提下，把更多信号接入动作建议与日报汇总

## 说明
这个仓库会继续同步更新。后续如果项目继续做出来，可以直接在同一个仓库里追加 commit、补文档、补真实源和 demo。
