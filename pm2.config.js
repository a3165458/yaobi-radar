module.exports = {
  apps: [{
    name: 'yaobi-radar',
    script: 'main.py',
    interpreter: 'python3',
    cwd: '/root/yaobi-radar',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '256M',
    env: {
      PYTHONUNBUFFERED: '1'
    },
    log_file: '/root/.pm2/logs/yaobi-radar-out.log',
    error_file: '/root/.pm2/logs/yaobi-radar-error.log',
    out_file: '/root/.pm2/logs/yaobi-radar-out.log',
    merge_logs: true,
    time: true
  }]
};