#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PWA Watchdog - monitors and restarts PWA if needed."""

import subprocess
import time
import os
import signal
import sys
import urllib.request

PWA_SCRIPT = "run_pwa.py"
PWA_PORT = 5555
CHECK_INTERVAL = 30  # seconds
MAX_CPU_PERCENT = 80
MAX_RESPONSE_TIME = 10  # seconds
RESTART_COOLDOWN = 60  # seconds after restart before checking again

pwa_process = None
last_restart = 0


def log(msg):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")


def get_pwa_pid():
    """Find PWA process PID."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", PWA_SCRIPT],
            capture_output=True, text=True
        )
        if result.stdout.strip():
            return int(result.stdout.strip().split()[0])
    except:
        pass
    return None


def get_cpu_usage(pid):
    """Get CPU usage of a process."""
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "%cpu="],
            capture_output=True, text=True
        )
        return float(result.stdout.strip())
    except:
        return 0


def check_health():
    """Check if PWA is responding."""
    try:
        start = time.time()
        req = urllib.request.Request(
            f"http://localhost:{PWA_PORT}/api/health",
            headers={"User-Agent": "Watchdog/1.0"}
        )
        with urllib.request.urlopen(req, timeout=MAX_RESPONSE_TIME) as resp:
            elapsed = time.time() - start
            if resp.status == 200:
                return True, elapsed
    except Exception as e:
        return False, str(e)
    return False, "Unknown error"


def kill_pwa():
    """Kill any running PWA processes."""
    try:
        subprocess.run(["pkill", "-f", PWA_SCRIPT], capture_output=True)
        time.sleep(2)
    except:
        pass


def start_pwa():
    """Start PWA process."""
    global pwa_process, last_restart

    kill_pwa()

    log("Starting PWA...")
    pwa_process = subprocess.Popen(
        [sys.executable, PWA_SCRIPT],
        stdout=open("pwa.log", "a"),
        stderr=subprocess.STDOUT,
        cwd=os.path.dirname(os.path.abspath(__file__))
    )
    last_restart = time.time()
    log(f"PWA started with PID {pwa_process.pid}")
    time.sleep(5)  # Give it time to start


def main():
    global last_restart

    log("PWA Watchdog started")
    log(f"Monitoring port {PWA_PORT}, check interval {CHECK_INTERVAL}s")

    # Start PWA initially
    start_pwa()

    while True:
        try:
            time.sleep(CHECK_INTERVAL)

            # Skip check if recently restarted
            if time.time() - last_restart < RESTART_COOLDOWN:
                continue

            pid = get_pwa_pid()

            if not pid:
                log("PWA not running! Restarting...")
                start_pwa()
                continue

            # Check CPU usage
            cpu = get_cpu_usage(pid)
            if cpu > MAX_CPU_PERCENT:
                log(f"PWA CPU too high: {cpu}%! Restarting...")
                start_pwa()
                continue

            # Check health endpoint
            healthy, info = check_health()
            if not healthy:
                log(f"PWA not responding: {info}! Restarting...")
                start_pwa()
                continue

            # All good
            if isinstance(info, float):
                log(f"OK - PID:{pid}, CPU:{cpu:.1f}%, Response:{info:.2f}s")

        except KeyboardInterrupt:
            log("Shutting down...")
            kill_pwa()
            break
        except Exception as e:
            log(f"Watchdog error: {e}")


if __name__ == "__main__":
    main()
