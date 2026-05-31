# 妖币雷达 (Yaobi Radar) - 开发任务

## 背景
在币安永续合约市场中，"妖币"是指突发放量拉盘、振幅极大的低市值币种。本系统目标是实时监控币安所有 USDT 永续合约，通过多维度评分模型检测妖币信号，并通过 Telegram 推送告警。

## 项目结构
在 `/root/yaobi-radar/` 目录下创建以下文件：

```
yaobi-radar/
├── config.json              # 配置文件
├── requirements.txt         # Python 依赖
├── main.py                  # 入口 + PM2 配置
└── yaobi/
    ├── __init__.py
    ├── config.py              # 配置加载
    ├── binance_client.py      # 币安 API 封装
    ├── scoring.py             # 妖币评分算法（核心）
    ├── scanner.py             # 扫描引擎
    ├── websocket_monitor.py   # WebSocket 实时监听
    ├── telegram_bot.py        # Telegram 推送
    └── data_store.py          # SQLite 数据存储
```

## 核心评分算法（0-100 分）

共 5 个维度，权重分配：

### 1. 成交量突增 (40 分)
- 计算方式：当前 5 分钟成交量 / 前 30 分钟 EMA 均值
- 评分标准：
  - >10x: 40 分
  - >5x: 32 分
  - >3x: 24 分
  - >2x: 16 分
  - >1.5x: 8 分
  - 否则: 0 分

### 2. 价格振幅 (25 分)
- 计算方式：取 5min/15min/1h 三个时间窗口的最大涨跌幅
- 评分标准：
  - >15%: 25 分
  - >10%: 20 分
  - >8%: 15 分
  - >5%: 10 分
  - >3%: 5 分
  - 否则: 0 分

### 3. 持仓量(OI)变化 (20 分)
- 计算方式：当前 OI vs 前一次扫描 OI 的变化率
- 评分标准：
  - >50%: 20 分
  - >30%: 16 分
  - >15%: 12 分
  - >5%: 8 分
  - >2%: 4 分
  - 否则: 0 分

### 4. 资金费率 (10 分)
- 计算方式：当前资金费率绝对值
- 评分标准：
  - >0.05%: 10 分
  - >0.03%: 8 分
  - >0.01%: 6 分
  - >0.005%: 3 分
  - 否则: 0 分

### 5. 市值/流动性 (5 分)
- 计算方式：基于 24h 成交额（quoteVolume）
- 评分标准：
  - <10M USDT: 5 分（优先低市值，高弹性）
  - 10-100M: 4 分
  - 100M-1B: 3 分
  - >1B: 1 分
  - 否则: 0 分

### 额外加分项（可选）
- OI-价格背离（价格上涨+OI下降）: -5 分（虚弱信号）
- 清算额突增: +3 分
- 连续多根 K 线同方向: +2 分

## 告警规则
- **妖币级别 (≥70分)**: 🚨 立即推送 Telegram
- **关注级别 (50-69分)**: ⚠️ 推送 Telegram
- **观察级别 (30-49分)**: ℹ️ 只记录到数据库，不推送
- **无关紧要 (<30分)**: 不处理
- **冷却时间**: 同一合约 30 分钟内不重复告警

## 数据采集方案

### REST API （每 60 秒全量扫描）
- `GET /fapi/v1/ticker/24hr` - 获取所有合约的 24h 数据（价格、成交量、涨跌幅）
- `GET /fapi/v1/openInterest` - 获取各合约持仓量（每个 symbol 单独调用）
- `GET /fapi/v1/fundingRate` - 获取各合约资金费率
- `GET /fapi/v1/klines` - 获取 K 线数据（5min/15min/1h 时间框）

### WebSocket 实时监听
- 订阅 `!markPrice@arr@1s` 或各个 symbol 的 `markPrice@1s`
- 用于补充实时价格数据
- WebSocket 断线后自动重连

## Telegram 推送格式

```
🚨 妖币出没 | {symbol} | 妖币分: {score}

📈 5分钟涨跌: {change_5m:+.2f}%
📊 成交量突增: {volume_ratio:.1f}x (近30分钟均值)
🏛️ 持仓量变化: {oi_change:+.2f}%
💰 资金费率: {funding_rate:+.4f}%
💵 当前价格: {price}
📏 24h成交额: {volume_24h}
📉 24h涨跌: {change_24h:+.2f}%

⏰ {timestamp} (UTC+8)
🔗 <a href="https://www.binance.com/en/futures/{symbol}">Binance 合约</a>
```

## 数据存储 (SQLite)
使用 SQLite 存储在 `/root/yaobi-radar/data/yaobi.db`：

### 表结构
1. `scan_results` - 每次扫描结果
   - id, symbol, score, volume_score, price_score, oi_score, funding_score, liquidity_score, timestamp
2. `alerts` - 告警记录
   - id, symbol, score, alert_level, message, timestamp
3. `price_history` - 价格历史（用于计算滚动窗口）
   - id, symbol, price, volume, timestamp
4. `oi_history` - 持仓量历史
   - id, symbol, open_interest, timestamp

## 配置文件 (config.json)
```json
{
    "binance": {
        "base_url": "https://fapi.binance.com",
        "ws_url": "wss://fstream.binance.com/ws",
        "api_key": "",
        "api_secret": "",
        "rate_limit": 1200
    },
    "scanning": {
        "interval_seconds": 60,
        "symbols": [],
        "exclude_symbols": [],
        "min_24h_volume_usdt": 1000000,
        "max_symbols": 500
    },
    "scoring": {
        "volume_weight": 40,
        "price_weight": 25,
        "oi_weight": 20,
        "funding_weight": 10,
        "liquidity_weight": 5,
        "alert_threshold": 50,
        "critical_threshold": 70
    },
    "cooldown_minutes": 30,
    "telegram": {
        "bot_token": "",
        "chat_id": "",
        "thread_id": null,
        "enabled": true
    },
    "database": {
        "path": "data/yaobi.db",
        "retention_days": 7
    },
    "websocket": {
        "enabled": true,
        "reconnect_delay": 5,
        "ping_interval": 30
    },
    "logging": {
        "level": "INFO",
        "file": "logs/yaobi.log"
    }
}
```

## 技术要求
- Python 3.11+
- 异步 IO (asyncio + aiohttp)
- 关键模块使用异步实现，避免阻塞
- WebSocket 使用 websockets 库
- 完善的错误处理和重试机制
- 日志输出到文件 + 控制台
- 支持信号处理 (SIGTERM/SIGINT) 正常退出

## 开发任务

请按照以下顺序完成开发：

1. **先创建 `requirements.txt`** - 列出所有依赖
2. **创建 `config.json`** - 使用上述配置结构，值使用默认值
3. **创建 `yaobi/config.py`** - 配置加载类
4. **创建 `yaobi/data_store.py`** - SQLite 数据库操作（异步）
5. **创建 `yaobi/binance_client.py`** - 币安 API 封装（aiohttp异步）
6. **创建 `yaobi/scoring.py`** - 评分算法实现
7. **创建 `yaobi/telegram_bot.py`** - Telegram 推送
8. **创建 `yaobi/websocket_monitor.py`** - WebSocket 实时监听
9. **创建 `yaobi/scanner.py`** - 扫描引擎（主逻辑）
10. **创建 `main.py`** - 程序入口，启动扫描 + WebSocket

注意：
- 所有代码必须完整可运行
- 不能有占位符或 TODO
- 处理币安 API rate limit（1200 请求/分钟）
- 异步代码使用 asyncio
- 提供详细的注释
