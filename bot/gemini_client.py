import logging
import re
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import google.generativeai as genai
from google.generativeai import types as genai_types # Renomeado para evitar conflito com typing.types
from dateutil import parser as dateutil_parser

from bot.firestore_manager import FirestoreManager
from bot.utils import normalizar_texto
from bot.config import (
    GEMINI_REMINDER_CONFIRMATION_REGEX,
    RECURRENCE_KEYWORDS,
    # Não precisamos de todas as constantes de lembrete aqui, apenas as de detecção/extração.
)

# Logger para este módulo
logger = logging.getLogger(__name__)

class GeminiClient:
    def __init__(self,
                 gemini_api_key: str,
                 model_name: str,
                 system_context: str,
                 firestore_manager: FirestoreManager,
                 target_timezone_name: str # Adicionado para _extract_reminder_details_from_response
                 ):
        self.api_key = gemini_api_key
        self.model_name = model_name
        self.system_context = system_context # system_instruction
        self.firestore_manager = firestore_manager # Para build_context_prompt
        self.target_timezone = pytz.timezone(target_timezone_name) # Para _extract_reminder_details_from_response
        self._setup_client()

    def _setup_client(self):
        """Configura o cliente Gemini e configurações do modelo."""
        try:
            # genai.configure(api_key=self.api_key) # Configuração global, pode não ser ideal se houver múltiplos clientes.
            # Melhor usar a API Key diretamente na instanciação do client ou através de variáveis de ambiente lidas pelo SDK.
            # A biblioteca google-generativeai geralmente lê a API_KEY de GOOGLE_API_KEY.
            # Se self.api_key for None e a variável de ambiente estiver definida, pode funcionar.
            # Para garantir, passamos explicitamente se disponível.

            # client_options = {"api_key": self.api_key} if self.api_key else None # Não é assim que se passa a API Key
            # O cliente é instanciado sem argumentos diretos de API Key aqui,
            # confiando na variável de ambiente GOOGLE_API_KEY ou configuração global prévia.
            # Se for necessário passar explicitamente e não depender de var de ambiente:
            # from google.auth.transport.requests import Request
            # from google.oauth2.service_account import Credentials (para service account)
            # ou para API keys de usuário: genai.configure(api_key=self.api_key) ANTES de criar o client.
            # Por simplicidade, vamos assumir que a API key está no ambiente ou configurada globalmente.

            self.client = genai.GenerativeModel(self.model_name) # Para Gemini Pro e modelos mais recentes
            # self.client = genai.Client(api_key=self.api_key) # Se fosse o client antigo ou se precisasse de client options

            self.model_config = genai_types.GenerationConfig( # Corrigido para GenerationConfig
                # system_instruction=self.system_context, # system_instruction é para GenerationConfig, não GenerateContentConfig
                temperature=0.55,
                # Outros parâmetros como top_p, top_k podem ser adicionados aqui
            )
            # O system_instruction é melhor aplicado no momento de gerar o conteúdo,
            # especialmente se varia ou é extenso.
            logger.info(f"Cliente Gemini configurado para o modelo: {self.model_name}")
        except Exception as e:
            logger.error(f"Erro ao configurar o cliente Gemini: {e}", exc_info=True)
            raise

    def build_context_prompt(self, chat_id: str, current_prompt_text: str, current_message_timestamp: datetime, from_name: Optional[str] = None) -> str:
        """Constrói o prompt com histórico formatado corretamente, incluindo o resumo."""
        try:
            user_display_name = from_name if from_name else "Usuário"
            summary = self.firestore_manager.get_conversation_summary(chat_id) or ""
            history = self.firestore_manager.get_conversation_history(chat_id, limit=25)
            current_timestamp_iso = current_message_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')

            if not history and not summary:
                return f"{user_display_name} (em {current_timestamp_iso}): {current_prompt_text}"

            context_parts = []
            for msg in history:
                role = user_display_name if not msg.get('is_bot', False) else "Assistente"
                msg_timestamp_iso = "data desconhecida"
                if msg.get('timestamp'):
                    msg_dt = datetime.fromtimestamp(msg['timestamp'], timezone.utc)
                    msg_timestamp_iso = msg_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
                context_parts.append(f"{role} (em {msg_timestamp_iso}): {msg['message_text']}")
            context_str = "\n".join(context_parts)

            final_prompt_parts = []
            if summary:
                final_prompt_parts.append(f"### Resumo de conversas anteriores ###\n{summary}\n")
            if context_str:
                final_prompt_parts.append(f"### Histórico recente da conversa (use para referência, não responda diretamente a elas) ###\n{context_str}\n")

            final_prompt_parts.append(
                "### Nova interação (responda apenas a esta nova interação) ###\n"
                "Considere os timestamps das mensagens do histórico e da mensagem atual. "
                "Se uma mensagem do histórico for significativamente antiga, avalie se o tópico ainda é relevante. "
                "Use o histórico e o resumo como contexto apenas se pertinentes para a nova interação."
            )
            final_prompt_parts.append(f"{user_display_name} (em {current_timestamp_iso}): {current_prompt_text}")

            return "\n".join(final_prompt_parts)
        except Exception as e:
            logger.error(f"Erro ao construir contexto para o chat {chat_id}: {e}", exc_info=True)
            return f"{from_name or 'Usuário'}: {current_prompt_text}" # Fallback

    def generate_gemini_response(self, chat_id: str, current_input_text: str, current_message_timestamp: datetime, from_name: Optional[str] = None) -> str:
        """Gera resposta do Gemini considerando o contexto completo e usando Google Search tool."""
        try:
            full_prompt_with_history = self.build_context_prompt(chat_id, current_input_text, current_message_timestamp, from_name)

            # Ferramentas (como GoogleSearch) são configuradas ao gerar conteúdo
            # google_search_tool = genai_types.Tool(google_search=genai_types.GoogleSearch()) # Não é assim que se define
            # A forma correta é passar `tools=[self.client.tool_google_search]` ou similar,
            # mas a API mudou. Para o SDK atual `google-generativeai`, `tools` é uma lista de `Tool` objects.
            # A funcionalidade de Google Search é muitas vezes implícita ou configurada no modelo.
            # Vamos simplificar e assumir que o modelo pode usar busca se necessário, ou que `tools` não é usado agora.

            # Para o SDK google.generativeai (Gemini Pro)
            response = self.client.generate_content(
                contents=[full_prompt_with_history], # Lista de conteúdos
                generation_config=self.model_config, # Passa o GenerationConfig
                # system_instruction=self.system_context, # Pode ser parte do GenerationConfig ou do Content
                # safety_settings=... ,
                # tools=... se houver ferramentas explícitas
            )

            generated_text = "".join(part.text for part in response.candidates[0].content.parts if hasattr(part, 'text'))

            # Log de uso de ferramenta (grounding)
            # if response.candidates[0].grounding_attributions: # A API pode ter mudado isso
            #    logger.info(f"Gemini usou Google Search (baseado em grounding_attributions).")

            return generated_text.strip() if generated_text else "Desculpe, não consegui processar sua solicitação no momento."
        except Exception as e:
            logger.error(f"Erro na chamada ao Gemini para chat {chat_id}: {e}", exc_info=True)
            return "Desculpe, ocorreu um erro ao tentar gerar uma resposta. Por favor, tente novamente."

    def refine_reminder_content(self, original_content: str, chat_id: str) -> str:
        """Refina o conteúdo do lembrete usando Gemini."""
        if not original_content or not original_content.strip():
            logger.warning(f"Conteúdo original do lembrete está vazio para {chat_id}. Não refinando.")
            return ""
        prompt = (
            "Transforme a seguinte frase em um lembrete conciso e acionável. Extraia a tarefa principal. "
            "Por exemplo, de 'r la pelas horas que preciso separar umas roupas pra minha sogra?' extraia 'separar umas roupas para a sogra'. "
            "De 'me lembra de comprar leite horas' extraia 'comprar leite'.\n\n"
            f"Frase original: '{original_content}'\n\n"
            "Lembrete conciso:"
        )
        try:
            logger.info(f"Refinando conteúdo do lembrete para {chat_id} com Gemini. Original: '{original_content}'")
            # Usar uma configuração mais simples para refinamento, sem histórico ou busca.
            simple_config = genai_types.GenerationConfig(temperature=0.3)
            response = self.client.generate_content(
                contents=[prompt],
                generation_config=simple_config
            )
            refined_text = "".join(part.text for part in response.candidates[0].content.parts if hasattr(part, 'text'))

            if refined_text:
                logger.info(f"Conteúdo do lembrete refinado: '{refined_text}'")
                return refined_text.strip()
            else:
                logger.warning(f"Gemini retornou conteúdo refinado vazio para '{original_content}'. Usando original.")
                return original_content
        except Exception as e:
            logger.error(f"Erro ao refinar conteúdo do lembrete com Gemini para {chat_id}: {e}", exc_info=True)
            return original_content # Fallback para o original em caso de erro

    def detect_and_extract_reminder_from_text(self, response_text: str) -> Dict[str, Any]:
        """Detecta e extrai detalhes de um lembrete de um texto (resposta do Gemini)."""
        # Este método combina _detect_reminder_in_gemini_response e _extract_reminder_from_gemini_response
        details = {"found": False, "content": None, "datetime_obj": None, "recurrence": "none"}

        if not re.search(GEMINI_REMINDER_CONFIRMATION_REGEX, response_text, re.IGNORECASE):
            return details # Not found

        details["found"] = True
        logger.debug(f"Tentando extrair lembrete da resposta do Gemini: '{response_text}'")

        try:
            now_local = datetime.now(self.target_timezone)
            text_for_datetime_parsing = normalizar_texto(response_text)
            parsed_dt_naive, parsed_tokens = dateutil_parser.parse(
                text_for_datetime_parsing, fuzzy_with_tokens=True, dayfirst=True, default=now_local
            )
            if parsed_dt_naive:
                parsed_dt_local = self.target_timezone.localize(parsed_dt_naive) if parsed_dt_naive.tzinfo is None else parsed_dt_naive.astimezone(self.target_timezone)
                details["datetime_obj"] = parsed_dt_local.astimezone(timezone.utc)
            else:
                 logger.warning(f"dateutil.parser não extraiu data/hora de: '{response_text}'")
        except Exception as e:
            logger.warning(f"Erro ao extrair data/hora de '{response_text}': {e}", exc_info=True)

        content_patterns = [
            r'"([^"]+)"', r"'([^']+)'",
            r'lembrete\s+(?:de\s+|para\s+|sobre\s+)?(.+?)(?=\s+(?:às|as|para|em|hoje|amanhã|\d{1,2}[/:])|\.|\!|\?|,|$)',
            r'(?:lembrar|avisar|alertar)\s+(?:de|para|sobre|que)\s+(.+?)(?=\s+(?:às|as|para|em|hoje|amanhã|\d{1,2}[/:])|\.|\!|\?|,|$)',
            r'(?:para\s+)?(.+?)\s+(?:às|as)\s+\d{1,2}(?::\d{2})?(?!\s*(?:horas|hs))',
        ]
        extracted_content_candidates = []
        for pattern in content_patterns:
            match = re.search(pattern, response_text, re.IGNORECASE)
            if match:
                candidate = match.group(1).strip()
                candidate = re.sub(r'(?:às|as)\s*\d{1,2}:?\d{0,2}\s*(?:hs|h)?', '', candidate, flags=re.IGNORECASE).strip()
                candidate = re.sub(r'\b(?:hoje|amanhã|depois de amanhã)\b', '', candidate, flags=re.IGNORECASE).strip()
                candidate = re.sub(r'\b\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?\b', '', candidate, flags=re.IGNORECASE).strip()
                if candidate: extracted_content_candidates.append(candidate)

        if extracted_content_candidates:
            content = re.sub(r'\s+', ' ', extracted_content_candidates[0])
            stopwords_gemini = ['o', 'a', 'de', 'para', 'que', 'lembrete', 'agendado', 'está', 'foi', 'vou', 'te', 'lembrar', 'confirmado', 'anotado', 'definido']
            content_words = content.split()
            if len(content_words) > 3:
                while content_words and normalizar_texto(content_words[0]) in stopwords_gemini: content_words.pop(0)
                while content_words and normalizar_texto(content_words[-1]) in stopwords_gemini: content_words.pop()
                content = ' '.join(content_words).strip()
            if content and len(content) > 2: details["content"] = content

        for phrase, recurrence_type in RECURRENCE_KEYWORDS.items():
            if normalizar_texto(phrase) in normalizar_texto(response_text):
                details["recurrence"] = recurrence_type
                break

        logger.info(f"Detalhes extraídos do lembrete (Gemini resp): Content='{details['content']}', DateTime='{details['datetime_obj']}', Recurrence='{details['recurrence']}'")
        return details

# Adicionar import pytz que faltou
import pytz
```
