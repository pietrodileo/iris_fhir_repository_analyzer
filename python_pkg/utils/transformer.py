from sentence_transformers import SentenceTransformer

class Transformer:
    def __init__(self,model: str = "all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model)

    def create_vector(self, data: dict | str) -> dict:
        if isinstance(data, dict):
            desc = data["description"]
        elif isinstance(data, str):
            desc = data 
            
        vector = self.model.encode(desc, normalize_embeddings=True).tolist()

        return {
            "description": desc,
            "vector": vector
        }
