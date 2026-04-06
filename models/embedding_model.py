import numpy as np

from services.openai_service import OpenAIService, USE_OPENAI


class EmbeddingModel:

    _model = None
    _device = None
    _openai = None

    def __init__(self):
        self.use_openai = USE_OPENAI

        if self.use_openai:
            if EmbeddingModel._openai is None:
                EmbeddingModel._openai = OpenAIService()
            self.openai = EmbeddingModel._openai
            self.model = None
            self.device = None
            return

        from sentence_transformers import SentenceTransformer
        import torch

        if EmbeddingModel._model is None:
            EmbeddingModel._device = "cuda" if torch.cuda.is_available() else "cpu"
            print(f"Loading embedding model on {EmbeddingModel._device}...")
            EmbeddingModel._model = SentenceTransformer(
                "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
                device=EmbeddingModel._device
            )
            print("Embedding model loaded.")

        self.model = EmbeddingModel._model
        self.device = EmbeddingModel._device


    def encode(self, texts):

        if isinstance(texts, str):
            texts = [texts]

        if self.use_openai:
            vectors = self.openai.embedding_batch(texts)
            return np.array(vectors, dtype=np.float32)

        return self.model.encode(
            texts,
            convert_to_numpy=True,
            batch_size=32
        )