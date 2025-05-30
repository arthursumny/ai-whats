import unicodedata
import re

def normalizar_texto(texto: str) -> str:
    """
    Normaliza um texto removendo acentos, convertendo para minúsculas,
    removendo caracteres especiais e espaços extras.
    """
    if texto is None:
        return ""
    # Remove acentos e caracteres especiais
    texto_normalizado = unicodedata.normalize('NFD', str(texto))
    texto_normalizado = texto_normalizado.encode('ascii', 'ignore').decode('utf-8')
    # Converte para minúsculas
    texto_normalizado = texto_normalizado.lower()
    # Remove espaços extras (incluindo múltiplos espaços e espaços no início/fim)
    texto_normalizado = re.sub(r'\s+', ' ', texto_normalizado).strip()
    return texto_normalizado
