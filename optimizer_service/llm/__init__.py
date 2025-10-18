from optimizer_service.core.config import settings
from .base_provider import BaseLLMProvider
from .gemma_provider import GemmaAPIProvider
from .llama_cpp_provider import LlamaCppProvider
from .vllm_provider import VLLMProvider


def get_llm_provider() -> BaseLLMProvider:
    """
    Фабричная функция, которая создает и возвращает нужный экземпляр LLM-провайдера
    в зависимости от настроек.
    """
    provider_type = settings.LLM_PROVIDER.lower()
    if provider_type == "llama_cpp":
        return LlamaCppProvider(
            host=settings.LLAMA_HOST,
            port=settings.LLAMA_PORT
        )
    elif provider_type == "vllm":
        return VLLMProvider(
            host=settings.VLLM_HOST,
            port=settings.VLLM_PORT,
        )
    elif provider_type == "gemma":
        return GemmaAPIProvider(
            api_key=settings.GEMMA_API_KEY
        )

    else:
        raise ValueError(f"Неизвестный LLM_PROVIDER: {settings.LLM_PROVIDER}")

llm_provider = get_llm_provider()