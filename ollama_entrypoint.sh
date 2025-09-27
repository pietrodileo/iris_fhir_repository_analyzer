#!/bin/bash

# Start Ollama in the background.
/bin/ollama serve &
# Record Process ID.
pid=$!

# Pause for Ollama to start.
sleep 5

echo "Retrieving model (llama3.1)..."
ollama pull gemma3:1b
ollama pull gemma2:2b
ollama pull llama3.2:1b
echo "Done."

# Wait for Ollama process to finish.
wait $pid