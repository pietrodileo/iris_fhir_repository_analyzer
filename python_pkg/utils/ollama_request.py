import requests
import json

class ollama_request:
    def __init__(self, api_url:str):
        self.api_url = api_url

    def get_response(self, content, model):
        # define payload
        """
        Get a response from the Ollama API.

        This method sends a request to the Ollama API with the
        model name and a message with the role 'user' and the
        content 'Hello, how are you?'. It then prints out the
        response from the API, which is a JSON object with the
        message content.

        :return: None
        """
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": content
                }
            ]
        }

        # Send HTTP request to the ollama API
        response = requests.post(self.api_url, json=payload, stream=True)

        # Check if response is ok
        if response.status_code == 200:
            # print("Streaming response from Ollama API:")
            for line in response.iter_lines(decode_unicode=True):
                if line: # ignore empty lines
                    try:
                        json_data = json.loads(line)
                        if "message" in json_data and "content" in json_data["message"]:
                            yield json_data["message"]["content"]
                    except json.JSONDecodeError as e:
                        yield f"Error decoding JSON: {e}. \nFailed to parse line: {line}"
        else:
            yield f"Error: {response.status_code} - {response.text}"