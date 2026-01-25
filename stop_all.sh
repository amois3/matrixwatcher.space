#!/bin/bash
# Matrix Watcher - Stop All Services
echo "=== Stopping Matrix Watcher Services ==="

pkill -f "main.py" 2>/dev/null && echo "Stopped: main.py"
pkill -f "run_pwa.py" 2>/dev/null && echo "Stopped: run_pwa.py"
pkill -f "pwa_watchdog.py" 2>/dev/null && echo "Stopped: pwa_watchdog.py"
pkill -f "oracle_instance_creator.py" 2>/dev/null && echo "Stopped: oracle_instance_creator.py"
pkill -f "cloudflared tunnel run" 2>/dev/null && echo "Stopped: cloudflared tunnel"

echo ""
echo "All services stopped."
