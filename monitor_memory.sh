#!/bin/bash

# Memory monitoring script for OpenAlex processing
# Usage: ./monitor_memory.sh [process_name]

PROCESS_NAME=${1:-openalex_processor}

echo "🔍 Monitoring memory usage for process containing: $PROCESS_NAME"
echo "📊 Logging to memory_usage.log"
echo "⚠️  Will alert if RSS memory exceeds 40GB"
echo ""

# Create log file
LOG_FILE="memory_usage.log"
echo "Timestamp,PID,VSZ(MB),RSS(MB),CPU%,Command" > $LOG_FILE

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Find processes matching the name
    PROCESSES=$(ps aux | grep "$PROCESS_NAME" | grep -v grep | grep -v monitor_memory)
    
    if [ ! -z "$PROCESSES" ]; then
        echo "$PROCESSES" | while read line; do
            PID=$(echo $line | awk '{print $2}')
            VSZ_KB=$(echo $line | awk '{print $5}')
            RSS_KB=$(echo $line | awk '{print $6}')
            CPU=$(echo $line | awk '{print $3}')
            CMD=$(echo $line | awk '{for(i=11;i<=NF;i++) printf $i" "; print ""}')
            
            # Convert to MB
            VSZ_MB=$((VSZ_KB / 1024))
            RSS_MB=$((RSS_KB / 1024))
            
            # Log to file
            echo "$TIMESTAMP,$PID,$VSZ_MB,$RSS_MB,$CPU%,$CMD" >> $LOG_FILE
            
            # Display current status
            printf "🕐 %s | PID: %s | RSS: %'d MB | VSZ: %'d MB | CPU: %s%%\n" \
                   "$TIMESTAMP" "$PID" "$RSS_MB" "$VSZ_MB" "$CPU"
            
            # Alert if memory is too high
            if [ $RSS_MB -gt 40000 ]; then
                echo "🚨 WARNING: RSS memory usage is ${RSS_MB} MB (>40GB)!"
                echo "🚨 WARNING: RSS memory usage is ${RSS_MB} MB (>40GB)!" >> $LOG_FILE
            fi
        done
    else
        echo "⏳ $TIMESTAMP | No $PROCESS_NAME processes found"
    fi
    
    echo "────────────────────────────────────────────────────────────────"
    sleep 30  # Check every 30 seconds
done
