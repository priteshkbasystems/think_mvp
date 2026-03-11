from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F


class SentimentModel:

    _model = None
    _tokenizer = None
    _device = None

    def __init__(self):

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

        self.labels = ["NEGATIVE", "NEUTRAL", "POSITIVE"]


    # -----------------------------------------
    # SINGLE PREDICTION
    # -----------------------------------------

    def predict(self, text):

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