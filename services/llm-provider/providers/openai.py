from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential
from services.llm_provider.providers.base import BaseLLMProvider
from services.llm_provider.config import settings
from services.llm_provider.logging_config import logger

class OpenAIProvider(BaseLLMProvider):
    def __init__(self):
        self.client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    async def generate(self, prompt: str, context: list[str]) -> str:
        logger.info("Sending generation request to OpenAI", model=settings.OPENAI_MODEL_NAME)
        
        system_content = "You are a helpful assistant. Answer the user's question using the provided context chunks. If you cannot find the answer, state that you don't know."
        user_content = f"Context:\n" + "\n---\n".join(context) + f"\n\nQuestion: {prompt}"
        
        response = await self.client.chat.completions.create(
            model=settings.OPENAI_MODEL_NAME,
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content}
            ],
            temperature=0.0
        )
        return response.choices[0].message.content
