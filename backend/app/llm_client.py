# backend/app/llm_client.py

import httpx
from typing import List, Dict, Any

OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
MODEL_NAME = "mistral"  # можно поменять на другую модель, если захочешь


async def call_llm(messages: List[Dict[str, str]]) -> str:
    """
    Вызов локальной модели через Ollama.
    messages — список словарей вида [{"role": "system", "content": "..."} , ...]
    Возвращает текст ответа assistant.
    """
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            OLLAMA_URL,
            json={
                "model": MODEL_NAME,
                "messages": messages,
                "stream": False,
            },
        )
        resp.raise_for_status()
        data: Dict[str, Any] = resp.json()
        # формат у Ollama: {"message": {"role": "assistant", "content": "..."}}
        return data["message"]["content"]
