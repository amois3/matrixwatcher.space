#!/bin/bash
# Matrix Watcher - Start All Services in Background
cd "$(dirname "$0")"

echo "=== Matrix Watcher - Starting All Services ==="

# Kill existing processes
echo "Stopping existing processes..."
pkill -f "main.py" 2>/dev/null
pkill -f "run_pwa.py" 2>/dev/null
pkill -f "pwa_watchdog.py" 2>/dev/null
pkill -f "oracle_instance_creator.py" 2>/dev/null
pkill -f "cloudflared tunnel run" 2>/dev/null
sleep 2

# Create logs directory
mkdir -p logs

# 1. Main sensor system
echo "Starting main.py..."
nohup python3 main.py > logs/main.log 2>&1 &
echo "  PID: $!"

# 2. PWA Watchdog (will start PWA automatically)
echo "Starting PWA watchdog..."
nohup python3 pwa_watchdog.py > logs/watchdog.log 2>&1 &
echo "  PID: $!"

# 3. Cloudflare Tunnel
echo "Starting Cloudflare tunnel..."
nohup cloudflared tunnel run matrix-watcher > logs/tunnel.log 2>&1 &
echo "  PID: $!"

# 4. Oracle Instance Creator (optional)
if [ -f "oracle_instance_creator.py" ]; then
    echo "Starting Oracle instance creator..."
    nohup python3 oracle_instance_creator.py > logs/oracle.log 2>&1 &
    echo "  PID: $!"
fi

sleep 3

echo ""
echo "=== All services started ==="
echo ""
echo "Check status:  ./status.sh"
echo "View logs:     tail -f logs/*.log"
echo "Stop all:      ./stop_all.sh"
echo ""
echo "Website: https://matrixwatcher.space/"
