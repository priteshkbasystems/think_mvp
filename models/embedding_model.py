from sentence_transformers import SentenceTransformer
import torch

class EmbeddingModel:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
            device=self.device
        )

    def encode(self, texts):
        return self.model.encode(texts, convert_to_tensor=True)