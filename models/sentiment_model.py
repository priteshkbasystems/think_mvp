from services.openai_service import OpenAIService, USE_OPENAI


class SentimentModel:

    _model = None
    _tokenizer = None
    _device = None
    _openai = None

    def __init__(self, use_openai=None):
        self.use_openai = USE_OPENAI if use_openai is None else bool(use_openai)
        self.labels = ["NEGATIVE", "NEUTRAL", "POSITIVE"]

        if self.use_openai:
            if SentimentModel._openai is None:
                SentimentModel._openai = OpenAIService()
            self.openai = SentimentModel._openai
            self.tokenizer = None
            self.model = None
            self.device = None
            return

        from transformers import AutoTokenizer, AutoModelForSequenceClassification
        import torch

        model_name = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
        if SentimentModel._model is None:
            SentimentModel._device = "cuda" if torch.cuda.is_available() else "cpu"
            print(f"Loading Sentiment Model on {SentimentModel._device}: {model_name}")
            SentimentModel._tokenizer = AutoTokenizer.from_pretrained(model_name)
            SentimentModel._model = AutoModelForSequenceClassification.from_pretrained(
                model_name
            ).to(SentimentModel._device)

        self.tokenizer = SentimentModel._tokenizer
        self.model = SentimentModel._model
        self.device = SentimentModel._device


    # -----------------------------------------
    # SINGLE PREDICTION
    # -----------------------------------------

    def predict(self, text):
        if self.use_openai:
            return self.openai.sentiment(text)

        import torch
        import torch.nn.functional as F

        encoded = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=512
        ).to(self.device)

        with torch.no_grad():
            output = self.model(**encoded)

        scores = F.softmax(output.logits, dim=1)[0]

        neg, neu, pos = scores.tolist()

        if pos >= neg and pos >= neu:
            return {"label": "POSITIVE", "score": pos}

        elif neg >= pos and neg >= neu:
            return {"label": "NEGATIVE", "score": neg}

        else:
            return {"label": "NEUTRAL", "score": neu}


    # -----------------------------------------
    # BATCH PREDICTION (FAST)
    # -----------------------------------------

    def predict_batch(self, texts, batch_size=32):
        if self.use_openai:
            return self.openai.sentiment_batch(texts)

        import torch
        import torch.nn.functional as F
        results = []

        for i in range(0, len(texts), batch_size):

            batch = texts[i:i+batch_size]

            encoded = self.tokenizer(
                batch,
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=512
            ).to(self.device)

            with torch.no_grad():
                output = self.model(**encoded)

            scores = F.softmax(output.logits, dim=1)

            for score in scores:

                neg, neu, pos = score.tolist()

                if pos >= neg and pos >= neu:
                    label = "POSITIVE"
                    conf = pos

                elif neg >= pos and neg >= neu:
                    label = "NEGATIVE"
                    conf = neg

                else:
                    label = "NEUTRAL"
                    conf = neu

                results.append({
                    "label": label,
                    "score": conf
                })

        return results