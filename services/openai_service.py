import hashlib
import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

from openai import OpenAI


DB_PATH = "/content/drive/MyDrive/THINK_MVP/04_Analysis_Output/transformation_cache.db"
USE_OPENAI = os.getenv("USE_OPENAI", "true").lower() in {"1", "true", "yes", "on"}

_SENTIMENT_TOPIC_MODEL = "openai/gpt-oss-20b:free"
_NARRATIVE_MODEL = "openai/gpt-oss-20b:free"
_EMBEDDING_MODEL = "nvidia/llama-nemotron-embed-vl-1b-v2:free"
_OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"


class OpenAIService:
    _client: Optional[OpenAI] = None

    def __init__(self, db_path: str = DB_PATH):
        if OpenAIService._client is None and USE_OPENAI:
            api_key = "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"
            if not api_key:
                raise RuntimeError("Set OPENROUTER_API_KEY to use OpenRouter")
            OpenAIService._client = OpenAI(
                api_key=api_key,
                base_url=_OPENROUTER_BASE_URL,
            )
        self.client = OpenAIService._client
        self.db_path = db_path
        self._ensure_cache_table()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _ensure_cache_table(self):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_response_cache (
                cache_key TEXT PRIMARY KEY,
                cache_type TEXT,
                model TEXT,
                request_hash TEXT,
                response_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _stable_hash(payload: Dict[str, Any]) -> str:
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _text_hash(text: str) -> str:
        return hashlib.md5((text or "").encode("utf-8")).hexdigest()

    def _get_ai_cache(self, cache_type: str, request_hash: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        cursor = conn.cursor()
        cache_key = f"{cache_type}:{request_hash}"
        cursor.execute(
            """
            SELECT response_json
            FROM ai_response_cache
            WHERE cache_key=?
            """,
            (cache_key,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def _set_ai_cache(self, cache_type: str, model: str, request_hash: str, response: Dict[str, Any]):
        conn = self._connect()
        cursor = conn.cursor()
        cache_key = f"{cache_type}:{request_hash}"
        cursor.execute(
            """
            INSERT OR REPLACE INTO ai_response_cache
            (cache_key, cache_type, model, request_hash, response_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (cache_key, cache_type, model, request_hash, json.dumps(response, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()

    def _chat_json(
        self,
        model: str,
        system_prompt: str,
        user_payload: Dict[str, Any],
        cache_type: str,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        if not USE_OPENAI:
            raise RuntimeError("USE_OPENAI is disabled")
        if self.client is None:
            raise RuntimeError("OpenAI client is not initialized")

        req_hash = self._stable_hash({"model": model, "payload": user_payload, "cache_type": cache_type})
        cached = self._get_ai_cache(cache_type, req_hash)
        if cached is not None:
            return cached

        last_error = None
        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=model,
                    temperature=0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                    ],
                )
                content = response.choices[0].message.content or "{}"
                data = json.loads(content)
                self._set_ai_cache(cache_type, model, req_hash, data)
                return data
            except Exception as exc:
                last_error = exc
                time.sleep(min(2 ** attempt, 5))

        raise RuntimeError(f"OpenAI request failed after retries: {last_error}")

    def sentiment(self, text: str) -> Dict[str, Any]:
        payload = {"text": text or ""}
        data = self._chat_json(
            model=_SENTIMENT_TOPIC_MODEL,
            system_prompt=(
                "Classify sentiment for the given text. Return JSON with keys: "
                "label (POSITIVE|NEGATIVE|NEUTRAL), score (0..1 float)."
            ),
            user_payload=payload,
            cache_type="sentiment",
        )
        label = str(data.get("label", "NEUTRAL")).upper()
        if label not in {"POSITIVE", "NEGATIVE", "NEUTRAL"}:
            label = "NEUTRAL"
        score = float(data.get("score", 0.0))
        score = max(0.0, min(1.0, score))
        return {"label": label, "score": score}

    def sentiment_batch(self, texts: List[str]) -> List[Dict[str, Any]]:
        if not texts:
            return []
        chunk_size = 15
        out: List[Dict[str, Any]] = []
        for i in range(0, len(texts), chunk_size):
            chunk = texts[i : i + chunk_size]
            payload = {"texts": chunk}
            data = self._chat_json(
                model=_SENTIMENT_TOPIC_MODEL,
                system_prompt=(
                    "Classify sentiment for each text. "
                    "Return JSON: {\"items\":[{\"label\":\"POSITIVE|NEGATIVE|NEUTRAL\",\"score\":0..1}, ...]} "
                    "matching input order."
                ),
                user_payload=payload,
                cache_type="sentiment_batch",
            )
            items = data.get("items", [])
            if not isinstance(items, list):
                items = []
            if len(items) != len(chunk):
                # Fallback to per-item for robust alignment.
                out.extend([self.sentiment(t) for t in chunk])
                continue
            for item in items:
                label = str(item.get("label", "NEUTRAL")).upper()
                if label not in {"POSITIVE", "NEGATIVE", "NEUTRAL"}:
                    label = "NEUTRAL"
                score = float(item.get("score", 0.0))
                score = max(0.0, min(1.0, score))
                out.append({"label": label, "score": score})
        return out

    def topic_classification(self, text: str, candidates: Optional[List[str]] = None) -> Dict[str, Any]:
        payload = {"text": text or "", "candidates": candidates or []}
        data = self._chat_json(
            model=_SENTIMENT_TOPIC_MODEL,
            system_prompt=(
                "Classify text into a single topic. "
                "If candidates are provided, choose one of them. "
                "Return JSON with key: topic."
            ),
            user_payload=payload,
            cache_type="topic",
        )
        topic = str(data.get("topic", "other")).strip() or "other"
        if candidates and topic not in candidates:
            topic = "other"
        return {"topic": topic}

    def topic_classification_batch(
        self, texts: List[str], candidates: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        if not texts:
            return []
        chunk_size = 15
        out: List[Dict[str, Any]] = []
        for i in range(0, len(texts), chunk_size):
            chunk = texts[i : i + chunk_size]
            payload = {"texts": chunk, "candidates": candidates or []}
            data = self._chat_json(
                model=_SENTIMENT_TOPIC_MODEL,
                system_prompt=(
                    "Classify each text into one topic. "
                    "If candidates exist, each topic must be from candidates. "
                    "Return JSON: {\"items\":[{\"topic\":\"...\"}, ...]} matching input order."
                ),
                user_payload=payload,
                cache_type="topic_batch",
            )
            items = data.get("items", [])
            if not isinstance(items, list) or len(items) != len(chunk):
                out.extend([self.topic_classification(t, candidates) for t in chunk])
                continue
            for item in items:
                topic = str(item.get("topic", "other")).strip() or "other"
                if candidates and topic not in candidates:
                    topic = "other"
                out.append({"topic": topic})
        return out

    def embedding(self, text: str) -> List[float]:
        text = text or ""
        text_hash = self._text_hash(text)

        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT embedding FROM embedding_cache WHERE text_hash=?", (text_hash,))
        row = cursor.fetchone()
        conn.close()
        if row:
            import numpy as np

            return np.frombuffer(row[0], dtype=np.float32).tolist()

        if not USE_OPENAI:
            raise RuntimeError("USE_OPENAI is disabled")
        if self.client is None:
            raise RuntimeError("OpenAI client is not initialized")

        response = self.client.embeddings.create(model=_EMBEDDING_MODEL, input=text)
        vector = response.data[0].embedding

        import numpy as np

        arr = np.array(vector, dtype=np.float32)
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO embedding_cache (text_hash, embedding)
            VALUES (?, ?)
            """,
            (text_hash, arr.tobytes()),
        )
        conn.commit()
        conn.close()
        return vector

    def embedding_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        return [self.embedding(t) for t in texts]

    def narrative_analysis(self, text: str) -> Dict[str, Any]:
        payload = {"text": text or ""}
        data = self._chat_json(
            model=_NARRATIVE_MODEL,
            system_prompt=(
                "Analyze transformation narrative. Return JSON with keys: "
                "summary (string), key_themes (array of strings), sentiment_label (Positive|Neutral|Negative), "
                "narrative_score (0..100 float), risks (array), opportunities (array)."
            ),
            user_payload=payload,
            cache_type="narrative",
        )
        return data

