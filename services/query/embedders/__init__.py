from services.query.config import settings

_embedder = None

def get_embedder():
    global _embedder
    if _embedder is None:
        if settings.EMBEDDING_PROVIDER == "local":
            from services.query.embedders.local import LocalEmbedder
            _embedder = LocalEmbedder()
        elif settings.EMBEDDING_PROVIDER == "openai":
            from services.query.embedders.openai import OpenAIEmbedder
            _embedder = OpenAIEmbedder()
        elif settings.EMBEDDING_PROVIDER == "ollama":
            from services.query.embedders.ollama import OllamaEmbedder
            _embedder = OllamaEmbedder()
        else:
            raise ValueError(f"Unknown embedding provider: {settings.EMBEDDING_PROVIDER}")
    return _embedder
