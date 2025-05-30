import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class WhatsAppClient:
    BASE_URL = "https://gate.whapi.cloud"

    def __init__(self, api_key: str):
        if not api_key:
            logger.error("WhatsAppClient: WHAPI_API_KEY não fornecida.")
            raise ValueError("WHAPI_API_KEY é obrigatória para WhatsAppClient.")
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        # Testa a conexão ao inicializar. Lança exceção se falhar.
        self.test_connection()

    def test_connection(self) -> bool:
        """Testa a conexão com a API Whapi.cloud."""
        try:
            response = requests.get(
                f"{self.BASE_URL}/settings",
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            logger.info("WhatsAppClient: Conexão com Whapi.cloud bem-sucedida.")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"WhatsAppClient: Falha na conexão com Whapi.cloud: {e}")
            raise  # Re-lança a exceção para que o chamador saiba da falha.

    def send_message(self, chat_id: str, text: str, reply_to: Optional[str] = None) -> bool:
        """Envia uma mensagem de texto para o WhatsApp."""
        if not text or not chat_id:
            logger.error("WhatsAppClient: Dados inválidos para envio de mensagem (chat_id ou texto ausente).")
            return False

        max_len = 4096
        if len(text) > max_len:
            logger.warning(f"WhatsAppClient: Mensagem para {chat_id} excedeu {max_len} caracteres. Será truncada.")
            text = text[:max_len-3] + "..."

        payload = {"to": chat_id, "body": text}
        if reply_to:
            payload["reply"] = reply_to

        try:
            response = requests.post(
                f"{self.BASE_URL}/messages/text",
                headers=self.headers,
                json=payload,
                timeout=20
            )
            response_data = response.json() # Tenta obter o JSON da resposta

            logger.info(f"WhatsAppClient: Resposta da API Whapi (Status {response.status_code}): {response_data}")

            # Whapi pode retornar 200 ou 201 para sucesso, mas também pode retornar 200 com um erro interno
            # Exemplo de erro interno com status 200: {'error': {'message': 'chat not found', 'type': 'WHAPI_EXCEPTION_GATE_CHAT_NOT_FOUND'}}
            if response.status_code >= 200 and response.status_code < 300:
                if isinstance(response_data, dict) and response_data.get('error'):
                    error_details = response_data.get('error')
                    logger.error(f"WhatsAppClient: Erro na API Whapi (Status {response.status_code}) apesar de parecer sucesso: {error_details}")
                    return False # Considerar como falha se houver um campo 'error'
                return True # Sucesso se status code é 2xx e não há 'error' no JSON
            else:
                response.raise_for_status() # Levanta erro para status >= 300 ou < 200 (exceto se já tratado)

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"WhatsAppClient: Erro HTTP ao enviar mensagem para {chat_id}: {http_err} - {response.text if response else 'Sem resposta'}")
        except requests.exceptions.RequestException as req_err:
            logger.error(f"WhatsAppClient: Erro de requisição ao enviar mensagem para {chat_id}: {req_err}")
        except Exception as e:
            logger.error(f"WhatsAppClient: Falha inesperada no envio da mensagem para {chat_id}: {e}", exc_info=True)

        return False
```
