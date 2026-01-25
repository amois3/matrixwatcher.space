#!/bin/bash
# Matrix Watcher - Status Check
echo "=== Matrix Watcher Status ==="
echo ""

check_process() {
    local name=$1
    local pattern=$2
    local pid=$(pgrep -f "$pattern" | head -1)
    if [ -n "$pid" ]; then
        local cpu=$(ps -p $pid -o %cpu= 2>/dev/null | tr -d ' ')
        local mem=$(ps -p $pid -o %mem= 2>/dev/null | tr -d ' ')
        echo "✅ $name (PID:$pid, CPU:${cpu}%, RAM:${mem}%)"
    else
        echo "❌ $name - NOT RUNNING"
    fi
}

check_process "Main Sensors" "main.py"
check_process "PWA Server" "run_pwa.py"
check_process "PWA Watchdog" "pwa_watchdog.py"
check_process "Cloudflare Tunnel" "cloudflared tunnel run"
check_process "Oracle Creator" "oracle_instance_creator.py"

echo ""
echo "=== Website Check ==="
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 https://matrixwatcher.space/ 2>/dev/null)
if [ "$HTTP_CODE" = "200" ]; then
    echo "✅ https://matrixwatcher.space/ - OK ($HTTP_CODE)"
else
    echo "❌ https://matrixwatcher.space/ - ERROR ($HTTP_CODE)"
fi

echo ""
echo "=== Recent L3+ Clusters ==="
curl -s http://localhost:5555/api/levels 2>/dev/null | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    levels=d.get('levels',[])[:3]
    if levels:
        for l in levels:
            print(f\"  L{l['level']}: {l['sources_str']} @ {l['time_str']}\")
    else:
        print('  No L3+ clusters yet')
except:
    print('  API not responding')
" 2>/dev/null

echo ""
