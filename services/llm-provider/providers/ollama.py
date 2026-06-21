import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from services.llm_provider.providers.base import BaseLLMProvider
from services.llm_provider.config import settings
from services.llm_provider.logging_config import logger

class OllamaProvider(BaseLLMProvider):
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    async def generate(self, prompt: str, context: list[str]) -> str:
        logger.info("Sending generation request to local Ollama", model=settings.OLLAMA_MODEL_NAME, url=settings.OLLAMA_URL)
        
        system_content = "You are a helpful assistant. Answer the user's question using the provided context chunks. If you cannot find the answer, state that you don't know."
        user_content = "Context:\n" + "\n---\n".join(context) + f"\n\nQuestion: {prompt}"
        
        payload = {
            "model": settings.OLLAMA_MODEL_NAME,
            "messages": [
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content}
            ],
            "stream": False,
            "options": {
                "temperature": 0.0
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{settings.OLLAMA_URL}/api/chat",
                json=payload,
                timeout=30.0
            )
            if response.status_code != 200:
                raise RuntimeError(f"Ollama returned error status {response.status_code}: {response.text}")
                
            return response.json()["message"]["content"]
