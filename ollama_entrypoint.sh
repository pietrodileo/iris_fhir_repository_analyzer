#!/bin/sh
set -e

# Start Ollama in the background
/bin/ollama serve &
pid=$!

# Pause for Ollama to start
sleep 5

echo "Retrieving models..."
ollama pull gemma3:1b
ollama pull gemma2:2b
ollama pull llama3.2:1b
echo "Done."

# Wait for Ollama process to finish
wait $pid
