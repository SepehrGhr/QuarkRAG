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
        else:
            raise ValueError(f"Unknown embedding provider: {settings.EMBEDDING_PROVIDER}")
    return _embedder
