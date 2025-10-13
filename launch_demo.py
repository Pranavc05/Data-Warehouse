#!/usr/bin/env python3
"""
🎬 Quick Demo Launcher - Multiple Demo Options
Launch different demo interfaces for different audiences
"""

import subprocess
import sys
import webbrowser
import time

def main():
    print("""
🚀 AUTOSQL DEMO LAUNCHER
========================

Choose your demo format:

1. 🌐 Web Dashboard (Professional/Interviews)
   - Interactive Streamlit interface
   - Perfect for live presentations
   - Charts, metrics, AI demonstrations

2. 🖥️  Terminal Demo (Technical/CLI)
   - Command-line demonstration
   - Shows system working end-to-end
   - Great for technical deep-dives

3. 📊 Jupyter Notebook (Data Science)
   - Interactive notebook with analysis
   - Step-by-step code walkthrough
   - Perfect for technical interviews

4. 🎯 All Demos (Full Stack)
   - Launch everything simultaneously
   - Multiple browser tabs
   - Show complete system architecture

Choice (1-4): """, end='')
    
    choice = input().strip()
    
    if choice == '1':
        launch_web_dashboard()
    elif choice == '2':
        launch_terminal_demo()
    elif choice == '3':
        launch_jupyter_demo()
    elif choice == '4':
        launch_all_demos()
    else:
        print("❌ Invalid choice. Launching web dashboard by default...")
        launch_web_dashboard()

def launch_web_dashboard():
    print("🌐 Launching Web Dashboard...")
    print("📍 URL: http://localhost:8501")
    subprocess.Popen(['streamlit', 'run', 'dashboard/main.py', '--server.port', '8501'])
    time.sleep(3)
    webbrowser.open('http://localhost:8501')
    print("✅ Web Dashboard launched! Browser should open automatically.")

def launch_terminal_demo():
    print("🖥️  Launching Terminal Demo...")
    subprocess.run(['python3', 'demo.py'])

def launch_jupyter_demo():
    print("📊 Creating Jupyter Demo Notebook...")
    # Create a quick Jupyter notebook for data science demos
    notebook_content = '''
{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# 🚀 AutoSQL: AI-Powered Data Warehouse\\n",
    "## Advanced SQL + AI Demo for Technical Interviews"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Import libraries\\n",
    "import pandas as pd\\n",
    "import numpy as np\\n",
    "from demo import AutoSQLDemo\\n",
    "\\n",
    "print('🤖 AutoSQL System Ready!')\\n",
    "demo = AutoSQLDemo()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python", 
   "name": "python3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}'''
    
    with open('AutoSQL_Demo.ipynb', 'w') as f:
        f.write(notebook_content)
    
    print("📊 Jupyter notebook created: AutoSQL_Demo.ipynb")
    print("💡 Run: jupyter notebook AutoSQL_Demo.ipynb")

def launch_all_demos():
    print("🎯 Launching ALL DEMOS...")
    
    # Web Dashboard
    print("1️⃣ Starting Web Dashboard...")
    subprocess.Popen(['streamlit', 'run', 'dashboard/main.py', '--server.port', '8501'])
    
    # Wait and open browser
    time.sleep(3)
    webbrowser.open('http://localhost:8501')
    
    print("✅ All demos launched!")
    print("""
🌐 Web Dashboard: http://localhost:8501
🖥️  Terminal Demo: Run 'python3 demo.py' in another terminal
📊 Jupyter Demo: Run 'jupyter notebook AutoSQL_Demo.ipynb'
    """)

if __name__ == "__main__":
    main()
