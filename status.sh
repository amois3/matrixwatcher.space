#!/bin/bash
# Matrix Watcher - Status Check
#
# Process discovery scopes to THIS project's directory (via /proc/<pid>/cwd)
# so we don't false-match unrelated python processes elsewhere on the machine.

set -u

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd -P)"

echo "=== Matrix Watcher Status ==="
echo ""

# Find the PID of a process whose command line matches $1 AND whose cwd is
# inside PROJECT_DIR. Echoes the first matching PID, or nothing.
find_pid() {
    local pattern=$1
    for pid in $(pgrep -f "$pattern"); do
        local cwd
        cwd=$(readlink "/proc/$pid/cwd" 2>/dev/null) || continue
        case "$cwd" in
            "$PROJECT_DIR"*) echo "$pid"; return 0;;
        esac
    done
}

check_process() {
    local name=$1
    local pattern=$2
    local pid
    pid=$(find_pid "$pattern")
    if [ -n "$pid" ]; then
        local cpu mem
        cpu=$(ps -p "$pid" -o %cpu= 2>/dev/null | tr -d ' ')
        mem=$(ps -p "$pid" -o %mem= 2>/dev/null | tr -d ' ')
        echo "✅ $name (PID:$pid, CPU:${cpu}%, RAM:${mem}%)"
    else
        echo "❌ $name - NOT RUNNING"
    fi
}

check_process "Main Sensors"      "main.py"
check_process "PWA Server"        "run_pwa.py"
check_process "PWA Watchdog"      "pwa_watchdog.py"

# Cloudflare tunnel is not project-scoped (no cwd we can pin to it sanely),
# so check it by command line only.
tunnel_pid=$(pgrep -f "cloudflared tunnel run" | head -1)
if [ -n "$tunnel_pid" ]; then
    echo "✅ Cloudflare Tunnel (PID:$tunnel_pid)"
else
    echo "❌ Cloudflare Tunnel - NOT RUNNING"
fi

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
import sys, json
try:
    d = json.load(sys.stdin)
    levels = d.get('levels', [])[:3]
    if levels:
        for l in levels:
            print(f\"  L{l['level']}: {l['sources_str']} @ {l['time_str']}\")
    else:
        print('  No L3+ clusters yet')
except Exception:
    print('  API not responding')
" 2>/dev/null

echo ""
