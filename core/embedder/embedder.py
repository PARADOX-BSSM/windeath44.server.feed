import os
from langchain_openai import OpenAIEmbeddings
from dotenv import load_dotenv

load_dotenv()

class Embedder:
    def __init__(self, model : str = "text-embedding-3-large"):
        self.emb = OpenAIEmbeddings(model=model, api_key=os.getenv("OPENAI_API_KEY"))

    def embed_text(self, text: str) -> list[float]:
        return self.emb.embed_query(text)

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return self.emb.embed_documents(texts)



