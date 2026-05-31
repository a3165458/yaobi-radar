# 妖币雷达 (Yaobi Radar)

币安 USDT 永续合约妖币实时监控系统。通过多维度评分模型检测突发放量、价格异动的低市值币种，并通过 Telegram 推送告警。

## 工作原理

- **REST API 全量扫描**：每 60 秒扫描币安全部 USDT 永续合约
- **WebSocket 实时监听**：订阅 markPrice 实时价格数据
- **五维评分模型**：成交量突增(40%) + 价格振幅(25%) + 持仓量变化(20%) + 资金费率(10%) + 流动性(5%)
- **Telegram 告警推送**：妖币级 ≥70 分（🚨）/ 关注级 50-69 分（⚠️）
- **SQLite 数据存储**：扫描结果、告警记录、价格/OI 历史

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置

```bash
cp config.example.json config.json
# 编辑 config.json，填入 Telegram bot token 和 chat_id
```

### 3. 运行

```bash
python main.py
```

或使用 PM2 管理：

```bash
pm2 start pm2.config.js
pm2 save
```

## 配置文件说明

| 配置块 | 说明 |
|--------|------|
| `binance` | 币安 API 地址和限速 |
| `scanning` | 扫描范围、频率、最小交易量阈值 |
| `scoring` | 五维评分权重和告警阈值 |
| `telegram` | Bot token、chat_id、thread_id |
| `database` | SQLite 路径和数据保留天数 |
| `websocket` | WebSocket 开关和重连配置 |
| `logging` | 日志级别和文件路径 |

## 项目结构

```
yaobi-radar/
├── config.example.json       # 配置模板
├── requirements.txt          # Python 依赖
├── main.py                   # 程序入口
├── pm2.config.js             # PM2 部署配置
└── yaobi/
    ├── __init__.py
    ├── config.py             # 配置加载与校验
    ├── binance_client.py     # 币安 REST API 封装
    ├── scoring.py            # 五维评分算法
    ├── scanner.py            # 扫描引擎主逻辑
    ├── websocket_monitor.py  # WebSocket 实时监听
    ├── telegram_bot.py       # Telegram 告警推送
    └── data_store.py         # SQLite 数据存储
```

## 告警格式示例

```
🚨 妖币出没 | BTCUSDT | 妖币分: 78

📈 5分钟涨跌: +12.35%
📊 成交量突增: 8.5x (近30分钟均值)
🏛️ 持仓量变化: +45.20%
💰 资金费率: +0.0320%
💵 当前价格: 97,234.50
📏 24h成交额: 2.35B USDT
📉 24h涨跌: +18.42%

⏰ 2025-05-30 14:25:00 (UTC+8)
🔗 Binance 合约
```

## 技术栈

- Python 3.11+
- asyncio + aiohttp
- websockets
- SQLite (aiosqlite)

## License

MIT
