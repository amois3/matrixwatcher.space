#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run Matrix Watcher PWA server."""

import uvicorn
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
 print("🌐 Starting Matrix Watcher PWA...")
 print("📱 Open http://localhost:5555 in your browser")
 print("Press Ctrl+C to stop\n")
 
 uvicorn.run(
 "web.api:app",
 host="0.0.0.0",
 port=5555,
 reload=False,
 log_level="info"
)
