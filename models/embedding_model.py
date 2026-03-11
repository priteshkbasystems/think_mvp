from sentence_transformers import SentenceTransformer
import torch


class EmbeddingModel:

    _model = None
    _device = None

    def __init__(self):

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

        return self.model.encode(
            texts,
            convert_to_numpy=True,
            batch_size=32
        )