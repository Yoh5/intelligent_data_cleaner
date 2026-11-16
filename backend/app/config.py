from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "Intelligent Data Cleaner API"
    VERSION: str = "1.0.0"
    DEBUG: bool = False
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:5173"]
    # Couche agentique (conseiller LLM). Vide → l'app reste 100% rule-based.
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4o-mini"
    # Démo publique (serveur ouvert à tous) : la route /execute, qui fait tourner un script
    # envoyé par le client, n'est pas montée — un filtre par liste d'interdictions se
    # contourne, ce serait de l'exécution de code à distance. Et aucun LLM n'est appelé :
    # personne ne consomme le crédit du propriétaire.
    DEMO_PUBLIQUE: bool = False

    class Config:
        env_file = ".env"

    def model_post_init(self, __context) -> None:
        if self.DEMO_PUBLIQUE:
            self.OPENAI_API_KEY = ""


@lru_cache()
def get_settings():
    return Settings()


settings = get_settings()
