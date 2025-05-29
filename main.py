import os
import requests
from google import genai
from google.genai import types
from google.genai.types import Tool, GenerateContentConfig, GoogleSearch
import time
import re
import logging
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from datetime import datetime, timedelta, timezone
from dateutil import parser as dateutil_parser # Added for reminder date parsing
from dateutil.relativedelta import relativedelta # Added for recurrence
import unicodedata
import pytz
import random
import calendar

# Forçar timezone do ambiente (não altera o sistema, só o processo Python)
os.environ['TZ'] = 'America/Sao_Paulo'

# Garantir que datetime.now() sempre retorne hora de SP por padrão
time.tzset() if hasattr(time, 'tzset') else None


# Carrega variáveis do .env
load_dotenv()

def normalizar_texto(texto):
    texto = unicodedata.normalize('NFD', texto)
    texto = texto.encode('ascii', 'ignore').decode('utf-8')
    texto = texto.lower()
    texto = re.sub(r'\s+', ' ', texto)
    texto = texto.strip()
    return texto

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WhatsAppGeminiBot:
    PENDING_CHECK_INTERVAL = 2
    REENGAGEMENT_TIMEOUT = (60 * 60 * 24 * 2)  # 2 dias em segundos
    # REENGAGEMENT_MESSAGES não será mais usado para a lógica principal,
    # mas pode ser um fallback se a geração do Gemini falhar.
    FALLBACK_REENGAGEMENT_MESSAGES = [
        "Oi! Está tudo bem por aí? Posso ajudar com algo?",
        "Oi! Como posso ajudar você hoje?",
    ]

    # Reminder feature constants
    # Lists for cleaning reminder content
    leading_words_to_strip_normalized = [
        "de", "para", "que", "sobre", "do", "da", "dos", "das",
        "me", "mim", "nos", "pra", "pro", "pros", "pras"
    ]

    trailing_phrases_to_strip_normalized = [
        "as", "às", "hs", "hrs", "horas", "hora",
        "em", "no", "na", "nos", "nas",
        "para", "de", "do", "da", "dos", "das",
        "pelas", "pelos", "a", "o", "amanha",
        "hoje", "la", "lá", "por", "volta",
        "depois", "antes", "proximo", "proxima"
    ]

    GEMINI_REMINDER_CONFIRMATION_REGEX = r"""(?ix)
(
    # Padrões de confirmação de lembrete
    (?:
        # "lembrete está/foi agendado/criado/anotado"
        lembrete\s+(?:está|esta|foi|será|sera)\s+(?:agendado|criado|anotado|marcado|definido|configurado|certinho|pronto|ok)
        |
        # "agendei/criei/anotei um lembrete"
        (?:agendei|criei|anotei|marquei|defini|configurei)\s+(?:um\s+|o\s+)?lembrete
        |
        # "vou te lembrar/lembrarei"
        (?:vou\s+(?:te\s+)?lembrar|lembrarei|te\s+lembrarei|vou\s+(?:te\s+)?avisar|avisarei)
        |
        # "está certinho/pronto/ok"
        (?:está|esta|tá|ta)\s+(?:confirmado|anotado|agendado)
        |
        # "não esqueça/esquecerei"
        (?:não\s+(?:vou\s+)?esquecer|nao\s+(?:vou\s+)?esquecer|pode\s+deixar|deixa\s+comigo)
        |
        # "lembrete para X às Y"
        lembrete\s+(?:de\s+|para\s+|sobre\s+)?.+?(?:às|as|para)\s+\d{1,2}(?::\d{2})?
        |
        # "te lembro/aviso X"
        te\s+(?:lembro|aviso|alerto|notifico)\s+(?:de\s+|para\s+|sobre\s+)?
        |
        # "anotado para X"
        (?:anotado|agendado|marcado)\s+para
        |
        # "X está/foi agendado"
        .+?\s+(?:está|esta|foi)\s+(?:agendado|anotado|marcado)
    )
)
"""

    REMINDER_STATE_AWAITING_CONTENT = "awaiting_content"
    REMINDER_STATE_AWAITING_DATETIME = "awaiting_datetime"
    REMINDER_STATE_AWAITING_RECURRENCE = "awaiting_recurrence" # Not actively used for asking, but for session state
    REMINDER_STATE_AWAITING_TIME = "awaiting_time"  # New state for when only time is missing
    REMINDER_STATE_AWAITING_CANCELLATION_CHOICE = "awaiting_cancellation_choice" # For cancellation flow
    REMINDER_SESSION_TIMEOUT_SECONDS = 300  # 5 minutes for pending reminder creation session
    REMINDER_CANCELLATION_SESSION_TIMEOUT_SECONDS = 300 # 5 minutes for pending cancellation session
    REMINDER_CHECK_INTERVAL_SECONDS = 60 # Check for due reminders every 60 seconds
    TARGET_TIMEZONE_NAME = 'America/Sao_Paulo'

    REMINDER_CONFIRMATION_TEMPLATES = [
        "Claro! Lembrete agendado para {datetime_str}:\n\n*{content}*",
        "Entendido! Seu lembrete para {datetime_str} está configurado:\n\n*{content}*",
        "Anotado! Te lembrarei em {datetime_str} sobre o seguinte:\n\n*{content}*",
        "Perfeito! Lembrete definido para {datetime_str}:\n\n*{content}*",
        "Confirmado! Agendei seu lembrete para {datetime_str}:\n\n*{content}*"
    ]

    REMINDER_CANCEL_KEYWORDS_REGEX = r"""(?ix)
    (?:cancelar|cancela|excluir|exclui|remover|remove)\s+
    (?:o\s+|meu\s+|um\s+)?
    (?:lembrete|agendamento)
    (?:\s+de\s+.*|\s+com\s+id\s+\w+)? # Optional: "lembrete de tomar agua" or "lembrete com id X"
    |
    (?:cancelar|cancela|excluir|exclui|remover|remove)\s+
    todos\s+(?:os\s+)?(?:meus\s+)?lembretes
"""

    PORTUGUESE_DAYS_FOR_PARSING = {
        "segunda": "monday", "terça": "tuesday", "quarta": "wednesday",
        "quinta": "thursday", "sexta": "friday", "sábado": "saturday", "domingo": "sunday",
        "segunda-feira": "monday", "terça-feira": "tuesday", "quarta-feira": "wednesday",
        "quinta-feira": "thursday", "sexta-feira": "friday"
    }

    MONTHLY_DAY_SPECIFIC_REGEX = r"""(?ix)
    \b(?:
        (?:todo\s+dia|mensalmente\s+(?:no\s+)?dia|dia) # "todo dia 10", "mensalmente dia 10", "dia 10" (when context implies monthly)
        \s+(\d{1,2})                                  # The day number (1-31)
        (?:
            \s+(?:de\s+cada\s+m[eê]s|por\s+m[eê]s)     # Optional "de cada mes", "por mes"
        )?
    |
        (?:todo\s+m[eê]s|mensalmente)\s+dia\s+(\d{1,2}) # "todo mes dia 10"
    )\b
    """

    RECURRENCE_KEYWORDS = {
        "diariamente": "daily", "todo dia": "daily", "todos os dias": "daily",
        "semanalmente": "weekly", "toda semana": "weekly", "todas as semanas": "weekly",
        "mensalmente": "monthly", "todo mes": "monthly", "todos os meses": "monthly", # "mes" without accent for easier regex
        "anualmente": "yearly", "todo ano": "yearly", "todos os anos": "yearly"
    }

    def __init__(self):
        self.reload_env()
        self.db = firestore.Client(project="voola-ai") # Seu projeto
        self.pending_timeout = 30  # Timeout para mensagens pendentes (em segundos)

        # FORÇAR o uso do timezone de São Paulo independente do servidor
        import pytz
        self.target_timezone = pytz.timezone('America/Sao_Paulo')

        # Verificar e log do timezone atual
        logger.info(f"=== INICIALIZAÇÃO TIMEZONE ===")
        logger.info(f"Sistema: {datetime.now().astimezone().tzinfo}")
        logger.info(f"Target: {self.target_timezone}")
        logger.info(f"Hora SP: {datetime.now(self.target_timezone)}")
        logger.info(f"Hora UTC: {datetime.now(timezone.utc)}")
        logger.info(f"=============================")

        if not all([self.whapi_api_key, self.gemini_api_key]):
            raise ValueError("Chaves API não configuradas no .env")

        self.setup_apis()
        self.pending_reminder_sessions: Dict[str, Dict[str, Any]] = {}
        self.pending_cancellation_sessions: Dict[str, Dict[str, Any]] = {}

    def _get_pending_messages(self, chat_id: str) -> Dict[str, Any]:
        """Obtém mensagens pendentes para um chat"""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        return {}
    
    def _save_pending_message(self, chat_id: str, message_payload: Dict[str, Any], from_name: str):
        """
        Armazena mensagem temporariamente com timestamp.
        message_payload deve conter: type, content, original_caption, mimetype, timestamp, message_id
        """
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        # Usar transação para garantir consistência ao adicionar mensagens
        @firestore.transactional
        def update_in_transaction(transaction, doc_ref, new_message, user_from_name):
            snapshot = doc_ref.get(transaction=transaction)
            existing_data = snapshot.to_dict() if snapshot.exists else {}
            
            messages = existing_data.get('messages', [])
            messages.append(new_message)

            transaction.set(doc_ref, {
                'messages': messages,
                'last_update': datetime.now(timezone.utc), # Sempre atualiza o timestamp do documento
                'processing': existing_data.get('processing', False),
                'from_name': user_from_name
            }, merge=True) # Merge para não sobrescrever 'processing' se já estiver lá

        update_in_transaction(self.db.transaction(), doc_ref, message_payload, from_name)

    def _detect_reminder_in_gemini_response(self, response_text: str) -> Dict[str, Any]:
        """
        Detecta se a resposta do Gemini indica que um lembrete deve ser criado.
        Retorna detalhes extraídos se encontrado.
        """
        # Usar regex robusto ao invés de lista simples
        if re.search(self.GEMINI_REMINDER_CONFIRMATION_REGEX, response_text, re.IGNORECASE):
            logger.info(f"Padrão de confirmação de lembrete detectado na resposta do Gemini")
            return self._extract_reminder_from_gemini_response(response_text)

        return {"found": False}

    def _extract_reminder_from_gemini_response(self, response_text: str) -> Dict[str, Any]:
        """
        Extrai detalhes do lembrete da resposta do Gemini.
        """
        details = {
            "found": True,
            "content": None,
            "datetime_obj": None,
            "recurrence": "none"
        }

        # Padrões melhorados para extrair conteúdo
        content_patterns = [
            # Entre aspas
            r'"([^"]+)"',
            r"'([^']+)'",
            # Após palavras-chave de lembrete
            r'lembrete\s+(?:de\s+|para\s+|sobre\s+)?([^\.!?,]+?)(?:\s+(?:às|as|para|hoje|amanhã|em)\s+|\.|\!|\?|,|$)',
            r'(?:lembrar|avisar|alertar)\s+(?:de\s+|para\s+|sobre\s+|que\s+)?([^\.!?,]+?)(?:\s+(?:às|as|para|hoje|amanhã|em)\s+|\.|\!|\?|,|$)',
            # Padrão específico para "X às Y"
            r'(?:para\s+)?(.+?)\s+(?:às|as)\s+\d{1,2}(?::\d{2})?',
            # Conteúdo antes de indicadores de tempo
            r'(?:de\s+|para\s+)?(.+?)\s+(?:hoje|amanhã|depois)',
        ]

        for pattern in content_patterns:
            match = re.search(pattern, response_text, re.IGNORECASE)
            if match:
                content = match.group(1).strip()
                # Limpar o conteúdo extraído
                content = re.sub(r'\s+', ' ', content)  # Normalizar espaços
                # Remover palavras comuns que não devem fazer parte do conteúdo
                stopwords = ['o', 'a', 'de', 'para', 'que', 'lembrete', 'agendado', 'está', 'foi']
                content_words = content.split()
                # Só remover se não ficar muito curto
                if len(content_words) > 3:
                    content_words = [w for w in content_words if w.lower() not in stopwords]
                    content = ' '.join(content_words)

                if content and len(content) > 2:  # Conteúdo válido
                    details["content"] = content
                    logger.debug(f"Conteúdo extraído: '{content}'")
                    break
                
        # Extrair data/hora com padrões mais específicos
        datetime_patterns = [
            # Horário explícito
            r'(?:às|as)\s*(\d{1,2}):?(\d{0,2})',
            # Data e hora
            r'(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?\s*(?:às|as)?\s*(\d{1,2}):?(\d{0,2})?',
            # Palavras temporais
            r'\b(hoje|amanhã|depois\s+de\s+amanhã)\b',
        ]

        # Primeiro, tentar extrair horários específicos
        time_match = re.search(r'(?:às|as)\s*(\d{1,2}):?(\d{0,2})', response_text, re.IGNORECASE)
        date_indicators = re.findall(r'\b(hoje|amanhã|depois\s+de\s+amanhã)\b', response_text, re.IGNORECASE)

        try:
            now_local = datetime.now(self.target_timezone)
            parsed_dt = None

            if time_match:
                hour = int(time_match.group(1))
                minute = int(time_match.group(2)) if time_match.group(2) else 0

                # Determinar o dia
                if date_indicators:
                    date_word = date_indicators[0].lower()
                    if 'hoje' in date_word:
                        parsed_dt = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    elif 'amanhã' in date_word:
                        parsed_dt = (now_local + timedelta(days=1)).replace(hour=hour, minute=minute, second=0, microsecond=0)
                    elif 'depois' in date_word:
                        parsed_dt = (now_local + timedelta(days=2)).replace(hour=hour, minute=minute, second=0, microsecond=0)
                else:
                    # Se não há indicador de dia, assumir hoje se o horário ainda não passou
                    parsed_dt = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    if parsed_dt <= now_local:
                        parsed_dt += timedelta(days=1)

            if not parsed_dt:
                # Tentar parsing mais genérico
                cleaned_text = self._clean_text_for_parsing(response_text)
                parsed_dt_naive, _ = dateutil_parser.parse(
                    cleaned_text,
                    fuzzy_with_tokens=True,
                    dayfirst=True,
                    default=now_local.replace(hour=9, minute=0, second=0)
                )

                if parsed_dt_naive.tzinfo is None:
                    parsed_dt = self.target_timezone.localize(parsed_dt_naive)
                else:
                    parsed_dt = parsed_dt_naive.astimezone(self.target_timezone)

            # Converter para UTC
            if parsed_dt:
                details["datetime_obj"] = parsed_dt.astimezone(timezone.utc)
                logger.debug(f"Data/hora extraída: {parsed_dt} (UTC: {details['datetime_obj']})")

        except (ValueError, TypeError) as e:
            logger.debug(f"Não foi possível extrair data/hora da resposta: {e}")

        # Detectar recorrência
        for phrase, recurrence_type in self.RECURRENCE_KEYWORDS.items():
            if normalizar_texto(phrase) in normalizar_texto(response_text):
                details["recurrence"] = recurrence_type
                logger.debug(f"Recorrência detectada: {recurrence_type}")
                break
            
        return details


    def _delete_pending_messages(self, chat_id: str):
        """Remove mensagens processadas"""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        doc_ref.delete()

    def _message_exists(self, message_id: str) -> bool:
        """Verifica se a mensagem já foi processada (Firestore)"""
        doc_ref = self.db.collection("processed_messages").document(message_id)
        return doc_ref.get().exists

    def _deactivate_reminder_in_db(self, reminder_id: str) -> bool:
        """Marks a specific reminder as inactive in Firestore and adds a cancelled_at timestamp."""
        try:
            reminder_ref = self.db.collection("reminders").document(reminder_id)
            reminder_ref.update({
                "is_active": False,
                "cancelled_at": firestore.SERVER_TIMESTAMP
            })
            return True
        except Exception as e:
            logger.error(f"Erro ao desativar lembrete {reminder_id}: {e}", exc_info=True)
            return False
    def _get_active_reminders(self, chat_id: str, limit: Optional[int] = 50) -> List[Dict[str, Any]]:
        """Fetches active reminders for a user, ordered by time.
           If limit is None, fetches all active reminders.
        """
        try:
            query_base = (
                self.db.collection("reminders")
                .where(filter=FieldFilter("chat_id", "==", chat_id))
                .where(filter=FieldFilter("is_active", "==", True))
                .order_by("reminder_time_utc", direction=firestore.Query.ASCENDING)
            )
            if limit is not None:
                query = query_base.limit(limit)
            else:
                query = query_base # No limit

            docs = query.stream()
            reminders = []
            for doc in docs:
                data = doc.to_dict()
                data["id"] = doc.id 
                # Ensure reminder_time_utc is a datetime object
                if "reminder_time_utc" in data and isinstance(data["reminder_time_utc"], (int, float)):
                    # Firestore Timestamps might be read as seconds since epoch if not handled by client lib sometimes
                    data["reminder_time_utc"] = datetime.fromtimestamp(data["reminder_time_utc"], tz=timezone.utc)
                elif "reminder_time_utc" in data and isinstance(data["reminder_time_utc"], datetime) and data["reminder_time_utc"].tzinfo is None:
                    # Ensure it's timezone-aware (UTC)
                    data["reminder_time_utc"] = data["reminder_time_utc"].replace(tzinfo=timezone.utc)
                reminders.append(data)
            return reminders
        except Exception as e:
            logger.error(f"Erro ao buscar lembretes ativos para {chat_id}: {e}", exc_info=True)
            return []

    def _save_message(self, message_id: str, chat_id: str, text: str, from_name: str, msg_type: str = "text"):
        """Armazena a mensagem no Firestore"""
        doc_ref = self.db.collection("processed_messages").document(message_id)
        doc_ref.set({
            "chat_id": chat_id,
            "text_content": text, # Pode ser descrição de mídia
            "message_type": msg_type,
            "from_name": from_name,
            "processed_at": firestore.SERVER_TIMESTAMP
        })

    def _save_conversation_history(self, chat_id: str, message_text: str, is_bot: bool):
        """Armazena o histórico da conversa no Firestore."""
        try:
            # Armazena mensagens do usuário e do bot para contexto completo
            col_ref = self.db.collection("conversation_history")
            col_ref.add({
                "chat_id": chat_id,
                "message_text": message_text,
                "is_bot": is_bot, # Adicionado para diferenciar no build_context_prompt
                "timestamp": firestore.SERVER_TIMESTAMP,
                "summarized": False
            })
        except Exception as e:
            logger.error(f"Erro ao salvar histórico para o chat {chat_id}: {e}")

    def _get_conversation_history(self, chat_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Obtém histórico ordenado cronologicamente, excluindo mensagens já resumidas."""
        try:
            query = (
                self.db.collection("conversation_history")
                .where(filter=FieldFilter("chat_id", "==", chat_id))
                .where(filter=FieldFilter("summarized", "==", False))
                .order_by("timestamp", direction=firestore.Query.ASCENDING) # ASCENDING para ordem cronológica
                .limit_to_last(limit) # limit_to_last para pegar as mais recentes
            )
            docs = query.get() 

            history = []
            for doc in docs:
                data = doc.to_dict()
                doc_timestamp = data.get('timestamp')
                # Ensure timestamp is a datetime object before calling .timestamp()
                if isinstance(doc_timestamp, datetime):
                    history_timestamp = doc_timestamp.timestamp()
                elif doc_timestamp is None: # Handle missing timestamp if necessary
                    history_timestamp = None 
                    logger.warning(f"Documento {doc.id} sem timestamp no histórico.")
                else: # If it's already a float or int (e.g. from older data)
                    try:
                        history_timestamp = float(doc_timestamp)
                    except (ValueError, TypeError):
                        logger.warning(f"Timestamp inválido no documento {doc.id}: {doc_timestamp}")
                        history_timestamp = None
                        

                if 'message_text' in data:
                    history.append({
                        'message_text': data['message_text'],
                        'is_bot': data.get('is_bot', False), # Adicionado
                        'timestamp': history_timestamp # Armazena como Unix timestamp (float)
                    })
                else:
                    logger.warning(f"Documento ignorado (campo 'message_text' ausente): {doc.id}")
            return history
        except Exception as e:
            logger.error(f"Erro ao buscar histórico: {e}")
            return []

    def reload_env(self):
        """Recarrega variáveis do .env"""
        load_dotenv(override=True)
        self.whapi_api_key = os.getenv('WHAPI_API_KEY')
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        self.gemini_model_name = os.getenv('GEMINI_MODEL') # Renomeado para clareza
        self.gemini_context = os.getenv('GEMINI_CONTEXT', '').replace('\\n', '\n')
        
    def setup_apis(self):
        """Configura as conexões com as APIs"""
        try:
            self.client = genai.Client(api_key=self.gemini_api_key)
            
            self.model_config = types.GenerateContentConfig(
                system_instruction=self.gemini_context,
                temperature=0.55
            )

            self.test_whapi_connection()
        except Exception as e:
            logger.error(f"Erro na configuração das APIs: {e}")
            raise

    def update_conversation_context(self, chat_id: str, user_message: str, bot_response: str):
        """Atualiza o contexto (histórico) diretamente no Firestore"""
        try:
            self._save_conversation_history(chat_id, user_message, False) # Mensagem do usuário
            
            context_ref = self.db.collection("conversation_contexts").document(chat_id)
            context_ref.set({
                "last_updated": firestore.SERVER_TIMESTAMP,
                "last_user_message": user_message, # O user_message aqui é o texto consolidado
                "last_bot_response": bot_response
            }, merge=True)
        except Exception as e:
            logger.error(f"Erro ao atualizar contexto: {e}")

    def build_context_prompt(self, chat_id: str, current_prompt_text: str, current_message_timestamp: datetime, from_name: Optional[str] = None) -> str:
        """Constrói o prompt com histórico formatado corretamente, incluindo o resumo."""
        try:
            user_display_name = from_name if from_name else "Usuário"

            summary_ref = self.db.collection("conversation_summaries").document(chat_id)
            summary_doc = summary_ref.get()
            summary = summary_doc.get("summary") if summary_doc.exists else ""

            history = self._get_conversation_history(chat_id, limit=25) # Limite menor para prompt

            current_timestamp_iso = current_message_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')

            if not history and not summary:
                return f"{user_display_name}: {current_prompt_text}" # Adiciona prefixo Usuário

            # Ordenar cronologicamente já é feito por _get_conversation_history
            context_parts = []
            for msg in history:
                role = user_display_name if not msg.get('is_bot', False) else "Assistente"
                msg_timestamp_iso = "data desconhecida"
                if msg.get('timestamp'): # msg['timestamp'] é um Unix timestamp (float)
                    # Converte Unix timestamp (float, assumido UTC) para objeto datetime UTC
                    msg_dt = datetime.fromtimestamp(msg['timestamp'], timezone.utc)
                    msg_timestamp_iso = msg_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
                context_parts.append(f"{role} (em {msg_timestamp_iso}): {msg['message_text']}")
            context_str = "\n".join(context_parts)
            
            # Monta o prompt final
            final_prompt = []
            if summary:
                final_prompt.append(f"### Resumo de conversas anteriores ###\n{summary}\n")
            if context_str: 
                final_prompt.append(f"### Histórico recente da conversa, nao responda elas, apenas use para uma possível referencia a (com timestamps) ###\n{context_str}\n")
            
            final_prompt.append(
                "### Nova interação, responda apenas a esta nova interação. ###\n"
                "Considere os timestamps das mensagens do histórico e da mensagem atual. "
                "Se uma mensagem do histórico for significativamente antiga em relação à mensagem atual, "
                "avalie cuidadosamente se o tópico ainda é relevante e se faz sentido continuar ou referenciar essa conversa antiga."
                "Use o histórico e o resumo acima como contexto apenas se forem pertinentes para a nova interação. Mas responda apenas a essa mensagem."
            )
            final_prompt.append(f"{user_display_name} (em {current_timestamp_iso}): {current_prompt_text}")
            
            return "\n".join(final_prompt)

        except Exception as e:
            logger.error(f"Erro ao construir contexto para o chat {chat_id}: {e}")
            return f"{user_display_name}: {current_prompt_text}" # Fallback simples

    def test_whapi_connection(self):
        try:
            response = requests.get(
                "https://gate.whapi.cloud/settings", # Removida barra final se não necessária
                headers={"Authorization": f"Bearer {self.whapi_api_key}"},
                timeout=10
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Falha na conexão com Whapi.cloud: {e}")
            raise

    def process_whatsapp_message(self, message: Dict[str, Any]) -> None:

        message_id = message.get('id')
        if not message_id:
            logger.warning("Mensagem sem ID recebida, ignorando.")
            return

        if self._message_exists(message_id) and not self.pending_reminder_sessions.get(message.get('chat_id')):
            logger.info(f"Mensagem {message_id} já processada e não há sessão de lembrete pendente, ignorando.")
            return

        chat_id = message.get('chat_id')
        if self._message_exists(message_id) and \
            not self.pending_reminder_sessions.get(chat_id) and \
            not self.pending_cancellation_sessions.get(chat_id):
             logger.info(f"Mensagem {message_id} já processada e não há sessão pendente, ignorando.")
             return
        from_name = message.get('from_name', 'Desconhecido')
        msg_type_whapi = message.get('type', 'text')
        caption = message.get('caption')
        mimetype = message.get('mimetype')
        text_body = ""

        # Texto
        if 'text' in message and isinstance(message['text'], dict):
            text_body = message['text'].get('body', '')
        elif 'body' in message and isinstance(message['body'], str):
            text_body = message['body']

        
        # --- Reminder and Cancellation Flow Logic ---
        # Manter apenas as sessões pendentes e cancelamento
        if chat_id in self.pending_reminder_sessions:
            self._save_message(message_id, chat_id, text_body, from_name, "text")
            self._save_conversation_history(chat_id, text_body, False)
            self._handle_pending_reminder_interaction(chat_id, text_body, message_id)
            return 

        if chat_id in self.pending_cancellation_sessions: 
            self._save_message(message_id, chat_id, text_body, from_name, "text")
            self._save_conversation_history(chat_id, text_body, False)
            self._handle_pending_cancellation_interaction(chat_id, text_body, message_id)
            return 

        # Manter apenas cancelamento direto (não criação)
        if self._is_cancel_reminder_request(text_body):
            logger.info(f"Requisição de cancelamento de lembrete detectada para '{text_body}'")
            self._save_message(message_id, chat_id, text_body, from_name, "text")
            self._save_conversation_history(chat_id, text_body, False)
            self._initiate_reminder_cancellation(chat_id, text_body, message_id)
            return 

        # REMOVER a detecção de criação de lembrete aqui
        # if self._is_reminder_request(text_body):
        #     ...

        # --- End Reminder and Cancellation Flow Logic ---

        # If not a reminder flow, proceed with standard message processing (Gemini, etc.)
        if self._message_exists(message_id): # Re-check, as reminder flow might have saved it
             logger.info(f"Mensagem {message_id} já processada (após checagem de lembrete), ignorando para fluxo Gemini.")
             return

        # Lógica para mídia (imagem, áudio, etc.)
        media_url = None
        if msg_type_whapi == 'image' and 'image' in message:
            media_url = message['image'].get('link')
            logger.info(f"Imagem recebida: {media_url}")
        elif msg_type_whapi in ['audio', 'ptt'] and 'audio' in message:
            media_url = message['audio'].get('link')
            logger.info(f"audio recebido: {media_url}")
        elif msg_type_whapi == 'video' and 'video' in message:
            media_url = message['video'].get('link')
            logger.info(f"video recebido: {media_url}")
        elif msg_type_whapi == 'document' and 'document' in message:
            media_url = message['document'].get('link')
            logger.info(f"Documento recebido: {media_url}")
        elif msg_type_whapi == 'voice' and 'voice' in message:
            media_url = message['voice'].get('link')
            logger.info(f"Voice recebida: {media_url}")

        # Decidir tipo processado internamente e conteúdo principal
        processed_type_internal = 'text'
        content_to_store = text_body or ""

        if media_url:
            if msg_type_whapi == 'image':
                processed_type_internal = 'image'
                content_to_store = media_url
            elif msg_type_whapi in ['audio', 'ptt']:
                processed_type_internal = 'audio'
                content_to_store = media_url
            elif msg_type_whapi == 'voice':
                processed_type_internal = 'voice'
                content_to_store = media_url
            elif msg_type_whapi == 'document':
                processed_type_internal = 'document'
                content_to_store = media_url
            elif msg_type_whapi == 'video':
                processed_type_internal = 'video'
                content_to_store = media_url
            elif caption:
                content_to_store = caption
                logger.info(f"Mídia tipo {msg_type_whapi} com caption, tratando como texto '{caption}'. URL: {media_url}")
            else:
                logger.info(f"Mídia tipo {msg_type_whapi} sem caption, ignorando mídia. URL: {media_url}")
                # não altera content_to_store nem o tipo se não tem caption

        text_for_processed_log = caption or text_body or f"[{processed_type_internal} recebida]"
        self._save_message(message_id, chat_id, text_for_processed_log, from_name, msg_type_whapi)

        if processed_type_internal == 'text' and not content_to_store.strip():
            logger.info(f"Mensagem de texto vazia ou mídia não suportada sem caption para {chat_id}, ignorando.")
            return

        pending_payload = {
            'type': processed_type_internal,
            'content': content_to_store,
            'original_caption': caption,
            'mimetype': mimetype,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'message_id': message_id,
            'link': media_url
        }

        self._save_pending_message(chat_id, pending_payload, from_name) # Passar from_name aqui
        logger.info(f"Mensagem de {from_name} ({chat_id}) adicionada à fila pendente. Tipo: {processed_type_internal}.")

    def _handle_pending_cancellation_interaction(self, chat_id: str, text: str, message_id: str):
        """Handles user's choice when cancelling a reminder from a list."""
        if chat_id not in self.pending_cancellation_sessions:
            logger.warning(f"Nenhuma sessão de cancelamento pendente para {chat_id}")
            # Optionally send a message if this state is reached unexpectedly
            # self.send_whatsapp_message(chat_id, "Desculpe, não encontrei uma solicitação de cancelamento ativa.", reply_to=message_id)
            return

        session = self.pending_cancellation_sessions[chat_id]
        session["last_interaction"] = datetime.now(timezone.utc)
        user_input_normalized = normalizar_texto(text.strip())

        original_message_id_session = session.get("original_message_id", message_id)

        if user_input_normalized in ["cancelar", "cancela", "nenhum", "nao"]:
            del self.pending_cancellation_sessions[chat_id]
            response_text = "Ok, nenhum lembrete foi cancelado."
            self.send_whatsapp_message(chat_id, response_text, reply_to=original_message_id_session)
            self._save_conversation_history(chat_id, response_text, True)
            return

        reminders_options = session.get("reminders_options", []) # Lista de dicionários com 'id' e 'text_summary'

        if user_input_normalized == "todos":
            cancelled_count = 0
            if not reminders_options:
                 response_text = "Não há lembretes na lista para cancelar."
            else:
                for opt in reminders_options: # Cancel only from the presented list
                    if self._deactivate_reminder_in_db(opt["id"]):
                        cancelled_count += 1
                if cancelled_count > 0:
                    response_text = f"{cancelled_count} lembrete(s) da lista foram cancelados."
                else:
                    response_text = "Não foi possível cancelar os lembretes da lista. Tente novamente."
            
            del self.pending_cancellation_sessions[chat_id]
            self.send_whatsapp_message(chat_id, response_text, reply_to=original_message_id_session)
            self._save_conversation_history(chat_id, response_text, True)
            return

        # Handle single item case where user might say "sim" or "1"
        if len(reminders_options) == 1 and user_input_normalized in ["sim", "1", "s"]:
            reminder_to_cancel = reminders_options[0]
            if self._deactivate_reminder_in_db(reminder_to_cancel["id"]):
                response_text = f"Lembrete '{reminder_to_cancel['text_summary']}' foi cancelado."
            else:
                response_text = f"Não foi possível cancelar o lembrete '{reminder_to_cancel['text_summary']}'. Tente novamente."
            del self.pending_cancellation_sessions[chat_id]
            self.send_whatsapp_message(chat_id, response_text, reply_to=original_message_id_session)
            self._save_conversation_history(chat_id, response_text, True)
            return

        try:
            choice_index = int(user_input_normalized) - 1 # User input is 1-based
            if 0 <= choice_index < len(reminders_options):
                reminder_to_cancel = reminders_options[choice_index]
                if self._deactivate_reminder_in_db(reminder_to_cancel["id"]):
                    response_text = f"Lembrete '{reminder_to_cancel['text_summary']}' foi cancelado."
                else:
                    response_text = f"Não foi possível cancelar o lembrete '{reminder_to_cancel['text_summary']}'. Tente novamente."
                del self.pending_cancellation_sessions[chat_id]
                self.send_whatsapp_message(chat_id, response_text, reply_to=original_message_id_session)
                self._save_conversation_history(chat_id, response_text, True)
            else:
                response_text = "Opção inválida. Por favor, digite o número de um lembrete da lista, 'todos' (para os listados) ou 'nenhum'."
                self.send_whatsapp_message(chat_id, response_text, reply_to=message_id) # Reply to current message for correction
                self._save_conversation_history(chat_id, response_text, True)
        except ValueError: # Not a number (and not "todos", "sim", "nao", etc.)
            response_text = "Não entendi sua escolha. Por favor, digite o número de um lembrete da lista, 'todos' (para os listados) ou 'nenhum'."
            self.send_whatsapp_message(chat_id, response_text, reply_to=message_id) # Reply to current message for correction
            self._save_conversation_history(chat_id, response_text, True)


    def _initiate_reminder_cancellation(self, chat_id: str, text: str, message_id: str):
        """Handles the initial request to cancel reminders."""
        logger.info(f"Iniciando cancelamento de lembrete para {chat_id} com texto: '{text}'")

        if chat_id in self.pending_cancellation_sessions: # Clear any old session
            del self.pending_cancellation_sessions[chat_id]

        normalized_text = normalizar_texto(text)

        # Check if user explicitly wants to cancel ALL reminders
        if re.search(r'\btodos\b', normalized_text, re.IGNORECASE):
            all_active_reminders = self._get_active_reminders(chat_id, limit=None) # Fetch all
            if not all_active_reminders:
                response_text = "Você não possui lembretes ativos para cancelar."
                self.send_whatsapp_message(chat_id, response_text, reply_to=message_id)
                self._save_conversation_history(chat_id, response_text, True)
                return

            cancelled_count = 0
            for reminder in all_active_reminders:
                if self._deactivate_reminder_in_db(reminder["id"]):
                    cancelled_count += 1
            
            if cancelled_count > 0:
                response_text = f"{cancelled_count} lembrete(s) foram cancelados com sucesso."
            else:
                response_text = "Não encontrei lembretes ativos ou não foi possível cancelá-los. Tente novamente."
            self.send_whatsapp_message(chat_id, response_text, reply_to=message_id)
            self._save_conversation_history(chat_id, response_text, True)
            return

        active_reminders_for_listing = self._get_active_reminders(chat_id, limit=10)

        if not active_reminders_for_listing:
            response_text = "Você não possui lembretes ativos para cancelar."
            self.send_whatsapp_message(chat_id, response_text, reply_to=message_id)
            self._save_conversation_history(chat_id, response_text, True)
            return

        options_for_session = []
        response_parts = ["Você tem os seguintes lembretes ativos. Qual você gostaria de cancelar?"]
        
        for i, reminder in enumerate(active_reminders_for_listing):
            dt_utc = reminder["reminder_time_utc"]
            if isinstance(dt_utc, int) or isinstance(dt_utc, float): # Handle Firestore Timestamp as seconds
                 dt_utc = datetime.fromtimestamp(dt_utc, tz=timezone.utc)
            elif dt_utc.tzinfo is None: # Ensure dt_utc is timezone-aware
                dt_utc = timezone.utc.localize(dt_utc)
            
            dt_local = dt_utc.astimezone(self.target_timezone)
            
            formatted_time = dt_local.strftime('%d/%m/%Y às %H:%M')
            content_summary = reminder.get('content', 'Lembrete sem descrição')
            if len(content_summary) > 50:
                content_summary = content_summary[:47] + "..."
            
            summary = f"'{content_summary}' para {formatted_time}"
            response_parts.append(f"{i+1}. {summary}")
            options_for_session.append({"id": reminder["id"], "text_summary": summary})
        
        if len(active_reminders_for_listing) == 1:
             response_parts = [f"Você tem um lembrete ativo:\n1. {options_for_session[0]['text_summary']}\n"]
             response_parts.append("Digite '1' ou 'sim' para cancelá-lo, ou 'não'/'cancelar' para manter.")
        else:
            response_parts.append("\nDigite o número do lembrete para cancelar, 'todos' para cancelar os listados, ou 'nenhum'/'cancelar'.")
        
        response_text = "\n".join(response_parts)

        self.pending_cancellation_sessions[chat_id] = {
            "state": self.REMINDER_STATE_AWAITING_CANCELLATION_CHOICE,
            "reminders_options": options_for_session,
            "original_message_id": message_id,
            "last_interaction": datetime.now(timezone.utc)
        }
        self.send_whatsapp_message(chat_id, response_text, reply_to=message_id)
        self._save_conversation_history(chat_id, response_text, True)

    def _is_cancel_reminder_request(self, text: str) -> bool:
        """Checks if the text contains keywords indicating a reminder cancellation request."""
        if not text:
            return False
        # Normalize text for more reliable regex matching of keywords like "todos"
        normalized_text = normalizar_texto(text)
        return bool(re.search(self.REMINDER_CANCEL_KEYWORDS_REGEX, normalized_text, re.IGNORECASE))

    # --- Methods for Reminder Feature ---
    def _is_reminder_request(self, text: str) -> bool:
        """Checks if the text contains keywords indicating a reminder request."""
        if not text:
            return False
        return bool(re.search(self.REMINDER_REQUEST_KEYWORDS_REGEX, text, re.IGNORECASE))

    def _clean_text_for_parsing(self, text: str) -> str:
        """Prepares text for date/time parsing by translating Portuguese day names."""
        processed_text = text.lower()

        # Check for monthly day-specific pattern first
        monthly_match = re.search(self.MONTHLY_DAY_SPECIFIC_REGEX, processed_text)
        if monthly_match:
            day_num = monthly_match.group(1) or monthly_match.group(2)  # One of the groups will match
            if day_num and 1 <= int(day_num) <= 31:
                # Get current date in target timezone
                now_local = datetime.now(self.target_timezone)
                target_day = int(day_num)

                # Calculate next occurrence of this day
                if target_day < now_local.day:
                    # If target day has passed this month, move to next month
                    next_date = (now_local.replace(day=1) + relativedelta(months=1)).replace(day=target_day)
                else:
                    # Try this month first
                    try:
                        next_date = now_local.replace(day=target_day)
                    except ValueError:  # Invalid day for current month
                        next_date = (now_local.replace(day=1) + relativedelta(months=1)).replace(day=target_day)

                # Replace the matched text with the actual date
                date_str = next_date.strftime('%Y-%m-%d')
                processed_text = re.sub(monthly_match.group(0), date_str, processed_text)
                logger.info(f"Monthly day-specific pattern found. Converted to date: {date_str}")

        # Continue with regular day name translations
        for pt_day, en_day in self.PORTUGUESE_DAYS_FOR_PARSING.items():
            processed_text = re.sub(r'\b' + pt_day + r'\b', en_day, processed_text)

        # Handle "hoje", "amanhã", "depois de amanha"
        now_in_target_tz = datetime.now(self.target_timezone)
        today_date = now_in_target_tz.strftime('%Y-%m-%d')
        tomorrow_date = (now_in_target_tz + timedelta(days=1)).strftime('%Y-%m-%d')
        after_tomorrow_date = (now_in_target_tz + timedelta(days=2)).strftime('%Y-%m-%d')

        # Add timezone info to the date replacements
        processed_text = re.sub(r'\bhoje\b', f"{today_date} {self.target_timezone.zone}", processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'\bamanhã\b', f"{tomorrow_date} {self.target_timezone.zone}", processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'\bdepois de amanhã\b', f"{after_tomorrow_date} {self.target_timezone.zone}", processed_text, flags=re.IGNORECASE)

        # Convert various time formats to standard format
        # "HH e MM" -> "HH:MM"
        processed_text = re.sub(r'(\d{1,2})\s*e\s*(\d{1,2})', r'\1:\2', processed_text)
        # "as HH" -> "às HH:00"
        processed_text = re.sub(r'\b(?:as|às)\s+(\d{1,2})(?!\d|:)\b', r'\1:00', processed_text, flags=re.IGNORECASE)
        # Add seconds if not present
        processed_text = re.sub(r'(\d{1,2}:\d{2})(?!:\d{2})', r'\1:00', processed_text)

        # "próxima segunda" -> "next monday"
        processed_text = re.sub(r'próxima\s+', 'next ', processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'próximo\s+', 'next ', processed_text, flags=re.IGNORECASE)

        return processed_text

    def _extract_reminder_details_from_text(self, text: str, chat_id: str) -> Dict[str, Any]:
        """
        Extracts content, datetime, and recurrence from text with improved accuracy.
        Handles monthly day-specific patterns correctly.
        """
        details = {
            "content": None,
            "datetime_obj": None,
            "recurrence": "none",
            "day_of_month": None,  # For "monthly on day X"
            "time_explicitly_provided": False,
            "original_datetime_str": None
        }

        logger.info(f"Extracting reminder details from text: '{text}'")

        # 1. Initial cleanup: remove reminder keywords to isolate payload
        payload_text = re.sub(self.REMINDER_REQUEST_KEYWORDS_REGEX, "", text, flags=re.IGNORECASE).strip()
        logger.debug(f"After removing keywords: '{payload_text}'")

        # Remove common leading words/prepositions that might precede the actual content
        for word in self.leading_words_to_strip_normalized:
            pattern = r"^\s*" + re.escape(word) + r"\s+"
            payload_text = re.sub(pattern, "", normalizar_texto(payload_text), flags=re.IGNORECASE).strip()
        logger.debug(f"After removing leading words: '{payload_text}'")

        if not payload_text:
            logger.info("No payload text after initial cleanup")
            return details

        text_to_parse = payload_text

        # 2. Check for monthly day-specific pattern first
        monthly_match = re.search(self.MONTHLY_DAY_SPECIFIC_REGEX, text_to_parse)
        if monthly_match:
            day_num = monthly_match.group(1) or monthly_match.group(2)  # One of the groups will match
            if day_num and 1 <= int(day_num) <= 31:
                details["recurrence"] = "monthly"
                details["day_of_month"] = int(day_num)
                logger.info(f"Found monthly day-specific pattern. Day: {day_num}")
                # Remove the matched pattern from text_to_parse
                text_to_parse = re.sub(monthly_match.group(0), "", text_to_parse).strip()
        else:
            # 3. Extract other recurrence patterns if no monthly day-specific pattern
            found_recurrence_phrase = ""
            for phrase, key in self.RECURRENCE_KEYWORDS.items():
                normalized_phrase = normalizar_texto(phrase)
                normalized_text = normalizar_texto(text_to_parse)
                match = re.search(r'\b' + re.escape(normalized_phrase) + r'\b', normalized_text, re.IGNORECASE)
                if match:
                    original_phrase_match = re.search(r'\b' + re.escape(phrase) + r'\b', text_to_parse, re.IGNORECASE)
                    if original_phrase_match and len(original_phrase_match.group(0)) > len(found_recurrence_phrase):
                        found_recurrence_phrase = original_phrase_match.group(0)
                        details["recurrence"] = key
                        logger.debug(f"Found recurrence: {key} from phrase '{found_recurrence_phrase}'")

            if found_recurrence_phrase:
                text_to_parse = text_to_parse.replace(found_recurrence_phrase, "").strip()
                logger.debug(f"After removing recurrence: '{text_to_parse}'")

        # 4. Parse DateTime
        cleaned_for_datetime = self._clean_text_for_parsing(text_to_parse)
        try:
            now_local = datetime.now(self.target_timezone)
            logger.info(f"=== DEBUG TIMEZONE ===")
            logger.info(f"Sistema timezone: {datetime.now().astimezone().tzinfo}")
            logger.info(f"Target timezone: {self.target_timezone}")
            logger.info(f"Now local (São Paulo): {now_local}")
            logger.info(f"Now UTC: {datetime.now(timezone.utc)}")
            logger.info(f"Texto para parsing: '{cleaned_for_datetime}'")
            logger.info(f"==================")
            parsed_dt_naive, non_datetime_tokens = dateutil_parser.parse(
                cleaned_for_datetime,
                fuzzy_with_tokens=True,
                dayfirst=True,
                default=now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            )

            only_time_provided = all(
                token.strip().lower() not in cleaned_for_datetime.lower()
                for token in ['today', 'tomorrow', 'next', 'monday', 'tuesday', 'wednesday',
                            'thursday', 'friday', 'saturday', 'sunday']
            ) and not any(
                re.search(r'\d{1,2}[-/]\d{1,2}', token)
                for token in non_datetime_tokens
            )

            if parsed_dt_naive.tzinfo is None:
                parsed_dt = self.target_timezone.localize(parsed_dt_naive, is_dst=None)
            else:
                parsed_dt = parsed_dt_naive.astimezone(self.target_timezone)

            if only_time_provided:
                details["time_explicitly_provided"] = True
                if parsed_dt.time() < now_local.time():
                    parsed_dt = parsed_dt + timedelta(days=1)
                    logger.info(f"Only time provided and was past current time. Adjusted to next day: {parsed_dt}")

            # For monthly reminders with day_of_month, ensure correct date
            if details["recurrence"] == "monthly" and details["day_of_month"]:
                target_day = details["day_of_month"]
                target_time = parsed_dt.time()

                # Try to set the target day in current month first
                try:
                    target_date = now_local.replace(day=target_day)
                except ValueError:  # Invalid day for current month
                    # Move to next month and try again
                    target_date = (now_local.replace(day=1) + relativedelta(months=1)).replace(day=target_day)

                # If target date/time is in the past, move to next month
                target_datetime = target_date.replace(
                    hour=target_time.hour,
                    minute=target_time.minute,
                    second=0,
                    microsecond=0
                )

                if target_datetime <= now_local:
                    target_datetime = (target_datetime + relativedelta(months=1))

                parsed_dt = target_datetime

            details["datetime_obj"] = parsed_dt.astimezone(timezone.utc)
            logger.debug(f"Final parsed datetime (UTC): {details['datetime_obj']}")

            content_parts = [token.strip() for token in non_datetime_tokens if token.strip()]
            initial_content = " ".join(content_parts).strip()
            logger.debug(f"Initial content from non-datetime tokens: '{initial_content}'")

        except (ValueError, TypeError) as e:
            logger.info(f"DateTime parsing failed: {e}")
            initial_content = text_to_parse

        # 5. Clean up content
        if initial_content:
            content_words = initial_content.split()
            while content_words and any(
                normalizar_texto(content_words[-1]) == word
                for word in self.trailing_phrases_to_strip_normalized
            ):
                content_words.pop()
                logger.debug(f"Removed trailing word, remaining: '{' '.join(content_words)}'")

            cleaned_content = " ".join(content_words).strip()
            cleaned_content = re.sub(self.REMINDER_REQUEST_KEYWORDS_REGEX, "", cleaned_content, flags=re.IGNORECASE).strip()

            if cleaned_content and not any(
                normalizar_texto(cleaned_content) == word
                for word in self.trailing_phrases_to_strip_normalized + self.leading_words_to_strip_normalized
            ):
                details["content"] = cleaned_content
                logger.info(f"Final extracted content: '{cleaned_content}'")
            else:
                logger.info("Content was invalid or only contained common words")
                details["content"] = None

        return details

    def _initiate_reminder_creation(self, chat_id: str, text: str, message_id: str):
        """Starts the process of creating a new reminder."""
        logger.info(f"Initiating reminder creation for chat {chat_id} from text: {text}")
        
        # Clean up any previous stale session for this chat_id
        if chat_id in self.pending_reminder_sessions:
            del self.pending_reminder_sessions[chat_id]

        extracted_details = self._extract_reminder_details_from_text(text, chat_id)
        
        content = extracted_details.get("content")
        datetime_obj_utc = extracted_details.get("datetime_obj") # Já está em UTC
        recurrence = extracted_details.get("recurrence", "none")

        session_data = {
            "state": "",
            "content": content,
            "datetime_obj": datetime_obj_utc,
            "recurrence": recurrence,
            "original_message_id": message_id,
            "last_interaction": datetime.now(timezone.utc)
        }

        if not content:
            session_data["state"] = self.REMINDER_STATE_AWAITING_CONTENT
        elif not datetime_obj_utc:
            session_data["state"] = self.REMINDER_STATE_AWAITING_DATETIME

        if session_data["state"]:
            self.pending_reminder_sessions[chat_id] = session_data
            self._ask_for_missing_reminder_info(chat_id, session_data)
        else:
            # All details found
            refined_content = self._refine_reminder_content_with_gemini(content, chat_id)
            if not refined_content:
                logger.warning(f"Refinamento do conteúdo do lembrete '{content}' falhou ou retornou vazio. Usando conteúdo original.")
                refined_content = content

            self._save_reminder_to_db(chat_id, refined_content, datetime_obj_utc, recurrence, message_id)

            datetime_local = datetime_obj_utc.astimezone(self.target_timezone)
            datetime_local_str = datetime_local.strftime('%d/%m/%Y às %H:%M')

            confirmation_template = random.choice(self.REMINDER_CONFIRMATION_TEMPLATES)
            response_text = confirmation_template.format(datetime_str=datetime_local_str, content=refined_content)
            if recurrence != "none":
                response_text += f" (Recorrência: {recurrence})"
            self.send_whatsapp_message(chat_id, response_text, reply_to=message_id)
            self._save_conversation_history(chat_id, response_text, True)


    def _handle_pending_reminder_interaction(self, chat_id: str, text: str, message_id: str):
        """Handles user's response when the bot is waiting for more reminder info."""
        if chat_id not in self.pending_reminder_sessions:
            logger.warning(f"No pending reminder session for {chat_id} in _handle_pending_reminder_interaction")
            return

        session = self.pending_reminder_sessions[chat_id]
        session["last_interaction"] = datetime.now(timezone.utc)

        if text.lower().strip() in ["cancelar", "cancela"]:
            del self.pending_reminder_sessions[chat_id]
            response_text = "Criação de lembrete cancelada."
            self.send_whatsapp_message(chat_id, response_text, reply_to=message_id)
            self._save_conversation_history(chat_id, response_text, True)
            return

        current_state = session["state"]

        if current_state == self.REMINDER_STATE_AWAITING_CONTENT:
            if text.strip():
                session["content"] = text.strip()
                session["state"] = ""
            else:
                self.send_whatsapp_message(chat_id, "O conteúdo do lembrete não pode ser vazio. Por favor, me diga o que devo lembrar.", reply_to=message_id)
                self._save_conversation_history(chat_id, "O conteúdo do lembrete não pode ser vazio. Por favor, me diga o que devo lembrar.", True)
                return

        elif current_state == self.REMINDER_STATE_AWAITING_DATETIME:
            try:
                import pytz
                sp_tz = pytz.timezone('America/Sao_Paulo')
                now_local = datetime.now(sp_tz)

                # ADICIONAR LOGS DE DEBUG:
                logger.info(f"=== DEBUG PARSING DATETIME ===")
                logger.info(f"Input text: '{text}'")
                logger.info(f"Now local (SP): {now_local}")
                logger.info(f"Now UTC: {datetime.now(timezone.utc)}")

                cleaned_text = self._clean_text_for_parsing(text)

                # Parse with default to start of current day
                parsed_dt_naive = dateutil_parser.parse(
                    cleaned_text,
                    fuzzy=True,
                    dayfirst=True,
                    default=now_local.replace(hour=0, minute=0, second=0, microsecond=0)
                )

                # Check if only time was provided
                only_time_provided = all(
                    token.strip().lower() not in cleaned_text.lower()
                    for token in ['hoje', 'amanha', 'amanhã', 'proximo', 'próximo', 'segunda', 'terça', 'quarta',
                                'quinta', 'sexta', 'sabado', 'sábado', 'domingo']
                ) and not re.search(r'\d{1,2}[-/]\d{1,2}', cleaned_text)

                # Localize the parsed datetime
                if parsed_dt_naive.tzinfo is None:
                    parsed_dt = self.target_timezone.localize(parsed_dt_naive, is_dst=None)
                else:
                    parsed_dt = parsed_dt_naive.astimezone(self.target_timezone)

                # If only time was provided and it's before current time
                if only_time_provided and parsed_dt.time() < now_local.time():
                    parsed_dt = parsed_dt + timedelta(days=1)
                    logger.info(f"Only time was provided and it was past current time. Adjusted to next day: {parsed_dt}")

                session["datetime_obj"] = parsed_dt.astimezone(timezone.utc)
                session["state"] = ""

            except (ValueError, TypeError) as e:
                logger.info(f"Could not parse datetime from user input '{text}': {e}")
                response_text = (
                    "Não consegui entender a data/hora. Por favor, tente de novo usando um dos formatos:\n"
                    "- hoje às 14:30\n"
                    "- amanhã 09:00\n"
                    "- 25/12 18:00\n"
                    "- próxima segunda 10:00"
                )
                self.send_whatsapp_message(chat_id, response_text, reply_to=message_id)
                self._save_conversation_history(chat_id, response_text, True)
                return
            except Exception as e_general:
                logger.error(f"Erro inesperado ao parsear data/hora '{text}': {e_general}", exc_info=True)
                response_text = "Ocorreu um erro ao processar a data/hora. Por favor, tente novamente."
                self.send_whatsapp_message(chat_id, response_text, reply_to=message_id)
                self._save_conversation_history(chat_id, response_text, True)
                return
        
        # Check if all required fields are now filled
        if not session.get("content"):
            session["state"] = self.REMINDER_STATE_AWAITING_CONTENT
        elif not session.get("datetime_obj"):
            session["state"] = self.REMINDER_STATE_AWAITING_DATETIME
        
        if session["state"]: # Still something missing
            self._ask_for_missing_reminder_info(chat_id, session)
        else: # All info gathered
            content_to_refine = session["content"]
            refined_content = self._refine_reminder_content_with_gemini(content_to_refine, chat_id)
            if not refined_content:
                logger.warning(f"Refinamento do conteúdo do lembrete '{content_to_refine}' falhou ou retornou vazio. Usando conteúdo original.")
                refined_content = content_to_refine

            self._save_reminder_to_db(
                chat_id,
                refined_content,
                session["datetime_obj"],
                session.get("recurrence", "none"),
                session["original_message_id"]
            )

            dt_obj_utc = session["datetime_obj"]
            dt_local = dt_obj_utc.astimezone(self.target_timezone)
            datetime_local_str = dt_local.strftime('%d/%m/%Y às %H:%M')

            confirmation_template = random.choice(self.REMINDER_CONFIRMATION_TEMPLATES)
            response_text = confirmation_template.format(datetime_str=datetime_local_str, content=refined_content)
            if session.get("recurrence", "none") != "none":
                response_text += f" (Recorrência: {session['recurrence']})"
            
            self.send_whatsapp_message(chat_id, response_text, reply_to=session["original_message_id"])
            self._save_conversation_history(chat_id, response_text, True)
            if chat_id in self.pending_reminder_sessions: # Clean up session
                del self.pending_reminder_sessions[chat_id]

    def _ask_for_missing_reminder_info(self, chat_id: str, session_data: Dict[str, Any]):
        """Asks the user for the next piece of missing information."""
        state = session_data["state"]
        question = ""
        if state == self.REMINDER_STATE_AWAITING_CONTENT:
            question = "Ok! Qual é o conteúdo do lembrete? (O que devo te lembrar?)"
        elif state == self.REMINDER_STATE_AWAITING_DATETIME:
            question = "Entendido. Para quando devo agendar este lembrete? (Ex: amanhã às 10h, 25/12/2024 15:00, hoje 18:30)"
        elif state == self.REMINDER_STATE_AWAITING_RECURRENCE: # Optional: not currently triggered unless logic changes
            question = "Este lembrete deve se repetir? (Ex: diariamente, semanalmente, ou não)"
        
        if question:
            self.send_whatsapp_message(chat_id, question, reply_to=session_data["original_message_id"])
            self._save_conversation_history(chat_id, question, True)
        else:
            # This case should ideally not be reached if states are managed properly
            logger.error(f"Reached _ask_for_missing_reminder_info with no question to ask for state {state}, session: {session_data}")

    def _refine_reminder_content_with_gemini(self, original_content: str, chat_id: str) -> str:
        """Refines the reminder content using Gemini for clarity and conciseness."""
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
            response = self.client.models.generate_content(
                model=self.gemini_model_name,
                contents=[prompt],
                config=self.model_config
            )

            refined_text = ""
            if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
                for part in response.candidates[0].content.parts:
                    if hasattr(part, 'text'):
                        refined_text += part.text
            refined_text = refined_text.strip()

            if refined_text:
                logger.info(f"Conteúdo do lembrete refinado: '{refined_text}'")
                return refined_text
            else:
                logger.warning(f"Gemini retornou conteúdo refinado vazio para '{original_content}'. Usando original.")
                return original_content
        except Exception as e:
            logger.error(f"Erro ao refinar conteúdo do lembrete com Gemini para {chat_id}: {e}", exc_info=True)
            return original_content

    def _save_reminder_to_db(self, chat_id: str, content: str, reminder_time_utc: datetime, 
                             recurrence: str, original_message_id: str, 
                             day_of_month: Optional[int] = None):
        """Saves the complete reminder to Firestore."""
        try:
            # Garantir que reminder_time_utc está em UTC
            if reminder_time_utc.tzinfo is None:
                # Se não tem timezone, erro - deve ser corrigido antes
                logger.error(f"reminder_time_utc sem timezone! Assumindo São Paulo.")
                reminder_time_sp = self.target_timezone.localize(reminder_time_utc)
                reminder_time_utc = reminder_time_sp.astimezone(timezone.utc)
            elif reminder_time_utc.tzinfo != timezone.utc:
                # Converter para UTC se não estiver
                reminder_time_utc = reminder_time_utc.astimezone(timezone.utc)

            reminder_payload = {
                "chat_id": chat_id,
                "content": content,
                "reminder_time_utc": reminder_time_utc,  # Sempre UTC no banco
                "recurrence": recurrence,
                "is_active": True,
                "created_at": firestore.SERVER_TIMESTAMP,
                "last_sent_at": None,
                "original_message_id": original_message_id,
                "original_hour_utc": reminder_time_utc.hour,
                "original_minute_utc": reminder_time_utc.minute,
                "timezone": self.TARGET_TIMEZONE_NAME  # Salvar timezone para referência
            }

            if recurrence == "monthly" and day_of_month is not None:
                reminder_payload["original_day_of_month"] = day_of_month

            doc_ref = self.db.collection("reminders").document()
            doc_ref.set(reminder_payload)

            # Log com horário local para clareza
            reminder_time_local = reminder_time_utc.astimezone(self.target_timezone)
            logger.info(f"Lembrete salvo para {chat_id}: {content} @ {reminder_time_local.strftime('%d/%m/%Y %H:%M %Z')} (UTC: {reminder_time_utc.strftime('%Y-%m-%d %H:%M:%S')})")

        except Exception as e:
            logger.error(f"Erro ao salvar lembrete para {chat_id}: {e}", exc_info=True)
            self.send_whatsapp_message(chat_id, "Desculpe, não consegui salvar seu lembrete. Tente novamente mais tarde.", reply_to=original_message_id)
            self._save_conversation_history(chat_id, "Desculpe, não consegui salvar seu lembrete. Tente novamente mais tarde.", True)

    def _get_next_occurrence(self, last_occurrence_utc: datetime, recurrence: str, original_hour_utc: int, original_minute_utc: int) -> Optional[datetime]:
        """Calculates the next occurrence time for a recurring reminder."""
        next_occurrence = None
        # Ensure the base for calculation is the last occurrence but with the original time of day
        base_time = last_occurrence_utc.replace(hour=original_hour_utc, minute=original_minute_utc, second=0, microsecond=0)

        if recurrence == "daily":
            next_occurrence = base_time + timedelta(days=1)
        elif recurrence == "weekly":
            next_occurrence = base_time + timedelta(weeks=1)
        elif recurrence == "monthly":
            next_occurrence = base_time + relativedelta(months=1)
        elif recurrence == "yearly":
            next_occurrence = base_time + relativedelta(years=1)
        
        # Ensure it's in the future from the actual last_occurrence_utc time
        # This handles cases where adding interval might still be in the past if original time was late in day
        if next_occurrence and next_occurrence <= last_occurrence_utc:
             # If adding the interval didn't push it past the current time (e.g. monthly on 31st to Feb)
             # or if base_time + interval is still <= last_occurrence_utc (should not happen with timedelta > 0)
             # Re-evaluate based on current time to ensure it's truly next
             now_utc = datetime.now(timezone.utc)
             while next_occurrence <= now_utc: # Keep adding interval until it's in the future
                if recurrence == "daily": next_occurrence += timedelta(days=1)
                elif recurrence == "weekly": next_occurrence += timedelta(weeks=1)
                elif recurrence == "monthly": next_occurrence += relativedelta(months=1)
                elif recurrence == "yearly": next_occurrence += relativedelta(years=1)
                else: break # Should not happen

        return next_occurrence


    def _check_and_send_due_reminders(self):
        """Checks Firestore for due reminders and sends them."""
        now_utc = datetime.now(timezone.utc)
        try:
            reminders_query = (
                self.db.collection("reminders")
                .where(filter=FieldFilter("is_active", "==", True))
                .where(filter=FieldFilter("reminder_time_utc", "<=", now_utc))
            )
            due_reminders = reminders_query.stream()

            for reminder_doc in due_reminders:
                reminder_data = reminder_doc.to_dict()
                # Corrected: chat_id should be fetched from reminder_data["chat_id"]
                chat_id = reminder_data.get("chat_id") 
                content = reminder_data.get("content")
                
                if not chat_id:
                    logger.error(f"Lembrete ID {reminder_doc.id} não possui chat_id. Dados: {reminder_data}")
                    # Mark as inactive or log for investigation
                    self.db.collection("reminders").document(reminder_doc.id).update({"is_active": False, "error_log": "Missing chat_id"})
                    continue

                if not content: # Should not happen if saved correctly, but good to check
                    logger.error(f"Lembrete ID {reminder_doc.id} para chat {chat_id} não possui conteúdo. Dados: {reminder_data}")
                    self.db.collection("reminders").document(reminder_doc.id).update({"is_active": False, "error_log": "Missing content"})
                    continue

                recurrence = reminder_data.get("recurrence", "none")
                reminder_id = reminder_doc.id
                original_msg_id = reminder_data.get("original_message_id")
                
                # Firestore timestamps are datetime objects when read
                reminder_time_utc = reminder_data["reminder_time_utc"] 
                if reminder_time_utc.tzinfo is None: # Garantir que é UTC
                    reminder_time_utc = reminder_time_utc.replace(tzinfo=timezone.utc)

                # Para o log, podemos mostrar a hora local do lembrete
                reminder_time_local = reminder_time_utc.astimezone(self.target_timezone)
                logger.info(f"Enviando lembrete ID {reminder_id} para {chat_id}: '{content}' agendado para {reminder_time_local.strftime('%d/%m/%Y %H:%M:%S %Z')}")
                
                
                

                # Listas de variações para cada parte da mensagem
                saudacoes = ["Olá", "Ei", "Oii", "Oie", "Oi", "E aí"]
                mensagens = ["estou passando para te lembrar", "só um lembrete rápido", "passando para avisar", "queria te lembrar", "lembrete importante"]
                introducoes = ["Não esqueça de", "Lembre-se de", "Por favor, não esqueça de"]
                despedidas = ["Até logo", "Até mais", "Até breve", "Tchau"]
                emojis = ["🙂", "😊", "👍", "🌟", "✨", "🙌", "⏰"]

                saudacao = random.choice(saudacoes)
                mensagem = random.choice(mensagens)
                introducao = random.choice(introducoes)
                despedida = random.choice(despedidas)
                emoji = random.choice(emojis)
        
                # A mensagem para o usuário não inclui a hora, então não precisa de conversão aqui.
                # Mas se incluísse, seria:
                # local_reminder_time_for_msg = reminder_time_utc.astimezone(self.target_timezone)
                # message_to_send = f"Não esqueça de: {content} (agendado para {local_reminder_time_for_msg.strftime('%H:%M')})"
                message_to_send = (f"{saudacao}, {mensagem}!\n\n"
                                   f"{introducao}: {content}\n\n"
                                   f"{despedida} {emoji}")
                
                success = self.send_whatsapp_message(chat_id, message_to_send, reply_to=None)

                if success:
                    self._save_conversation_history(chat_id, message_to_send, True) # Log bot's reminder
                    
                    update_data = {"last_sent_at": firestore.SERVER_TIMESTAMP}
                    if recurrence == "none":
                        update_data["is_active"] = False
                    else:
                        original_hour = reminder_data.get("original_hour_utc", reminder_time_utc.hour)
                        original_minute = reminder_data.get("original_minute_utc", reminder_time_utc.minute)
                        
                        next_occurrence_utc = self._get_next_occurrence(reminder_time_utc, recurrence, original_hour, original_minute)
                        if next_occurrence_utc:
                            update_data["reminder_time_utc"] = next_occurrence_utc
                            next_occurrence_local = next_occurrence_utc.astimezone(self.target_timezone)
                            logger.info(f"Lembrete {reminder_id} (recorrência: {recurrence}) reagendado para {next_occurrence_local.strftime('%Y-%m-%d %H:%M:%S %Z')} (UTC: {next_occurrence_utc.strftime('%Y-%m-%d %H:%M:%S %Z')})")
                        else:
                            update_data["is_active"] = False 
                            logger.warning(f"Não foi possível calcular próxima ocorrência para lembrete {reminder_id}. Desativando.")
                    
                    self.db.collection("reminders").document(reminder_id).update(update_data)
                else:
                    logger.error(f"Falha ao enviar lembrete ID {reminder_id} para {chat_id}.")

        except Exception as e:
            logger.error(f"Erro ao verificar/enviar lembretes: {e}", exc_info=True)

    def _cleanup_stale_pending_reminder_sessions(self):
        """Cleans up pending reminder and cancellation sessions that have timed out."""
        now = datetime.now(timezone.utc)
        # Clean reminder creation sessions
        stale_reminder_sessions = []
        for chat_id, session_data in self.pending_reminder_sessions.items():
            last_interaction = session_data.get("last_interaction")
            if last_interaction and (now - last_interaction).total_seconds() > self.REMINDER_SESSION_TIMEOUT_SECONDS:
                stale_reminder_sessions.append(chat_id)

        for chat_id in stale_reminder_sessions:
            del self.pending_reminder_sessions[chat_id]

        # Clean cancellation sessions
        stale_cancellation_sessions = []
        for chat_id, session_data in list(self.pending_cancellation_sessions.items()): # Iterate over a copy
            last_interaction = session_data.get("last_interaction")
            if last_interaction and (now - last_interaction).total_seconds() > self.REMINDER_CANCELLATION_SESSION_TIMEOUT_SECONDS:
                stale_cancellation_sessions.append(chat_id)

        for chat_id in stale_cancellation_sessions:
            if chat_id in self.pending_cancellation_sessions:
                b = 0
            del self.pending_cancellation_sessions[chat_id]

    def _check_pending_messages(self, chat_id: str):
        """Verifica se deve processar as mensagens acumuladas para um chat específico."""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        try:
            doc = doc_ref.get()
            if not doc.exists:
                return

            data = doc.to_dict()
            if data.get('processing', False):
                return

            last_update_dt = data.get('last_update')
            if isinstance(last_update_dt, datetime): # Ensure it's a datetime object
                # Firestore Timestamps são timezone-aware (UTC)
                pass
            else: # Se for string (pode acontecer se algo salvar errado)
                try:
                    last_update_dt = datetime.fromisoformat(str(last_update_dt)).replace(tzinfo=timezone.utc)
                except:
                    logger.error(f"Formato de last_update inválido para {chat_id}, usando now.")
                    last_update_dt = datetime.now(timezone.utc)


            now = datetime.now(timezone.utc)
            
            # Verifica se existem mensagens
            if not data.get('messages'):
                doc_ref.delete() # Limpa se estiver vazio
                return

            # Tempo desde a última atualização (quando a última mensagem foi adicionada OU quando começou a processar)
            timeout_seconds = (now - last_update_dt).total_seconds()

            if timeout_seconds >= self.pending_timeout:
                @firestore.transactional
                def mark_as_processing(transaction, doc_ref_trans):
                    snapshot = doc_ref_trans.get(transaction=transaction)
                    if snapshot.exists and not snapshot.get('processing'):
                        transaction.update(doc_ref_trans, {'processing': True, 'last_update': firestore.SERVER_TIMESTAMP})
                        return True
                    return False

                if mark_as_processing(self.db.transaction(), doc_ref):
                    self._process_pending_messages(chat_id)

        except Exception as e:
            logger.error(f"Erro ao verificar mensagens pendentes para {chat_id}: {e}", exc_info=True)
            # Tentativa de resetar o estado de processamento em caso de erro aqui
            try:
                doc_ref.update({'processing': False})
            except Exception as e_update:
                 logger.error(f"Erro ao tentar resetar 'processing' para {chat_id}: {e_update}")


    def _process_pending_messages(self, chat_id: str):
        """Processa todas as mensagens acumuladas, incluindo mídias."""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        try:
            
            doc = doc_ref.get() # Obter os dados mais recentes
            if not doc.exists:
                logger.warning(f"Documento de mensagens pendentes para {chat_id} não encontrado ao iniciar processamento.")
                return

            data = doc.to_dict()
            pending_msg_list = data.get('messages', [])
            user_from_name = data.get('from_name', 'Usuário') # Fallback para 'Usuário'

            if not pending_msg_list:
                logger.warning(f"Nenhuma mensagem pendente encontrada para {chat_id} ao processar.")
                self._delete_pending_messages(chat_id) # Limpa se estiver vazio
                return

            
            # Ordenar por timestamp (string ISO guardada)
            try:
                pending_msg_list.sort(key=lambda x: datetime.fromisoformat(x['timestamp']))
            except (TypeError, ValueError) as e_sort:
                logger.error(f"Erro ao ordenar mensagens pendentes para {chat_id} por timestamp: {e_sort}. Usando ordem atual.")

            # Obter o timestamp da última mensagem do lote para a "nova interação"
            # Este será o timestamp de referência para a "mensagem atual" no prompt do Gemini.
            current_interaction_timestamp = datetime.now(timezone.utc) # Fallback
            if pending_msg_list: # Garante que a lista não está vazia
                try:
                    # O timestamp é armazenado como string ISO 8601 UTC
                    last_msg_ts_str = pending_msg_list[-1]['timestamp']
                    current_interaction_timestamp = datetime.fromisoformat(last_msg_ts_str)
                    # Assegurar que é timezone-aware (UTC), fromisoformat pode retornar naive se Z/offset não estiver presente
                    # No entanto, datetime.now(timezone.utc).isoformat() sempre inclui offset.
                    if current_interaction_timestamp.tzinfo is None:
                        current_interaction_timestamp = current_interaction_timestamp.replace(tzinfo=timezone.utc)
                except (ValueError, TypeError, IndexError) as e_ts_parse:
                    logger.warning(f"Não foi possível parsear o timestamp ('{last_msg_ts_str}') da última mensagem pendente para {chat_id}: {e_ts_parse}. Usando now().")
                    current_interaction_timestamp = datetime.now(timezone.utc)


            processed_texts_for_gemini = []
            all_message_ids = [msg['message_id'] for msg in pending_msg_list]

            for msg_data in pending_msg_list:
                msg_type = msg_data['type']
                content = msg_data['content'] # Texto ou media_url
                original_caption = msg_data.get('original_caption')
                mimetype = msg_data.get('mimetype')

                if msg_type == 'text':
                    if content and content.strip():
                        processed_texts_for_gemini.append(content.strip())
                elif msg_type in ['audio', 'image', 'voice', 'video', 'document']:
                    media_url = content
                    if not mimetype:
                        # Tentar inferir mimetype da URL como último recurso (pouco confiável)
                        # Idealmente, Whapi sempre envia mimetype.
                        try:
                            logger.info(f"Attempting to infer mimetype from URL: {media_url}")
                            file_ext = os.path.splitext(media_url.split('?')[0])[1].lower() # Remove query params
                            if file_ext == ".jpg" or file_ext == ".jpeg": mimetype = "image/jpeg"
                            elif file_ext == ".png": mimetype = "image/png"
                            elif file_ext == ".mp3": mimetype = "audio/mp3"
                            elif file_ext == ".oga": mimetype = "audio/ogg" # Comum para PTT
                            elif file_ext == ".opus": mimetype = "audio/opus"
                            elif file_ext == ".wav": mimetype = "audio/wav"
                            elif file_ext == ".mp4" or file_ext == "mp4": mimetype = "video/mp4"
                            elif file_ext == ".pdf": mimetype = "application/pdf"
                            else: logger.warning(f"Mimetype não fornecido e não pôde ser inferido da URL: {media_url}")
                        except Exception:
                            logger.warning(f"Falha ao tentar inferir mimetype da URL: {media_url}")
                    
                    if not mimetype:
                        logger.error(f"Mimetype não disponível para mídia {media_url} do chat {chat_id}. Pulando mídia.")
                        processed_texts_for_gemini.append(f"[Erro: Tipo de arquivo da mídia não identificado ({media_url})]")
                        if original_caption: processed_texts_for_gemini.append(f"Legenda original: {original_caption}")
                        continue
                    
                    file_part_uploaded = None
                    try:
                        logger.info(f"Baixando e enviando mídia para Gemini: {media_url} (mimetype: {mimetype})")
                        
                        # Cabeçalhos para request de mídia, Whapi pode exigir autenticação
                        media_req_headers = {}
                        if self.whapi_api_key: # Adicionar token se a Whapi proteger URLs de mídia
                             media_req_headers['Authorization'] = f"Bearer {self.whapi_api_key}"
                        
                        media_response = requests.get(media_url, stream=True, timeout=60, headers=media_req_headers)
                        media_response.raise_for_status()
                        media_response.raw.decode_content = True

                        image_bytes = requests.get(media_url).content
                        image = types.Part.from_bytes(data=image_bytes, mime_type=mimetype)

                    
                        prompt_for_media = "Descreva este arquivo de forma concisa e objetiva e me retorne apenas a descrição, nada além disso, nenhuma palavra a mais."
                        if msg_type == 'audio' or msg_type == 'voice':
                            prompt_for_media = "Transcreva este audio, exatamente como está e me retorne apenas a transcriçao nenhuma palavra a mais, apenas a transcriçao."
                        elif msg_type == 'document':
                            prompt_for_media = "Descreva este arquivo pdf de forma concisa e objetiva. Anote todas as informações relevantes e me retorne apenas a descrição, nada além disso."
                        
                        # Gerar descrição/transcrição
                        media_desc_response = self.client.models.generate_content(
                            model=self.gemini_model_name,
                            contents=[prompt_for_media, image],
                            config=self.model_config,
                        )
                        media_description = media_desc_response.text.strip()
                        
                        if msg_type == 'audio':
                            entry = f"{user_from_name} enviou um(a) {msg_type}"
                            entry += f": [Conteúdo processado da mídia: {media_description}], mantenha esse conteudo na resposta e envie entre *asteriscos*, abaixo disso um resumo também."
                        elif msg_type == 'image':
                            entry = f"{user_from_name} enviou um(a) {msg_type}"
                            entry += f": [Conteúdo processado da mídia: {media_description}]."
                        elif msg_type == 'voice':
                            entry = f"{user_from_name} enviou um audio"
                            entry += f": [Conteúdo processado da mídia: {media_description}], responda normalmente como se fosse uma mensagem de texto."
                        elif msg_type == 'video':
                            entry = f"{user_from_name} enviou um(a) {msg_type}"
                            entry += f": [Conteúdo processado da mídia: {media_description}]."
                        elif msg_type == 'document':
                            entry = f"{user_from_name} enviou um(a) {msg_type}"
                            entry += f": [Conteúdo processado da mídia: {media_description}]."
                        processed_texts_for_gemini.append(entry)

                    except requests.exceptions.RequestException as e_req:
                        logger.error(f"Erro de request ao baixar mídia {media_url} para {chat_id}: {e_req}")
                        processed_texts_for_gemini.append(f"[Erro ao baixar {msg_type} ({media_url})]")
                        if original_caption: processed_texts_for_gemini.append(f"Legenda original: {original_caption}")
                    except Exception as e_gemini:
                        logger.error(f"Erro ao processar mídia {media_url} com Gemini para {chat_id}: {e_gemini}", exc_info=True)
                        processed_texts_for_gemini.append(f"[Erro ao processar {msg_type} com Gemini ({media_url})]")
                        if original_caption: processed_texts_for_gemini.append(f"Legenda original: {original_caption}")
                    finally:
                        # Limpeza do arquivo no Gemini (se necessário e aplicável para genai.upload_file)
                        # A documentação sugere que `genai.upload_file` é para uso único e os arquivos
                        # são temporários. Se usar `client.files.create`, então `client.files.delete` seria necessário.
                        # Por segurança, pode-se tentar deletar, mas pode dar erro se já foi limpo.
                        if file_part_uploaded:
                            try:
                                # genai.delete_file(file_part_uploaded.name) # Descomentar se necessário
                                logger.info(f"Arquivo {file_part_uploaded.name} processado. (Limpeza no Gemini geralmente automática para upload_file)")
                            except Exception as e_delete:
                                logger.warning(f"Falha ao tentar deletar arquivo {file_part_uploaded.name} no Gemini: {e_delete}")
                                
            # Consolidar todos os textos processados
            full_user_input_text = "\n".join(processed_texts_for_gemini).strip()

            if not full_user_input_text:
                logger.info(f"Nenhum texto processável após processar mensagens pendentes para {chat_id}. Limpando e saindo.")
                self._delete_pending_messages(chat_id)
                return # Não há nada para responder

            
            # Gerar resposta do Gemini
            response_text = self.generate_gemini_response(full_user_input_text, chat_id, current_interaction_timestamp)

            # NOVO: Verificar se a resposta do Gemini indica criação de lembrete
            reminder_details = self._detect_reminder_in_gemini_response(response_text)
            
            if reminder_details.get("found"):
                logger.info(f"Lembrete detectado na resposta do Gemini para {chat_id}")
                
                # Se faltam detalhes, usar a mensagem original para complementar
                if not reminder_details.get("content") or not reminder_details.get("datetime_obj"):
                    original_details = self._extract_reminder_details_from_text(full_user_input_text, chat_id)
                    
                    if not reminder_details.get("content") and original_details.get("content"):
                        reminder_details["content"] = original_details["content"]
                    
                    if not reminder_details.get("datetime_obj") and original_details.get("datetime_obj"):
                        reminder_details["datetime_obj"] = original_details["datetime_obj"]
                
                # Se temos todos os detalhes necessários, criar o lembrete
                if reminder_details.get("content") and reminder_details.get("datetime_obj"):
                    # Garantir que o datetime está em UTC
                    datetime_utc = reminder_details["datetime_obj"]
                    if datetime_utc.tzinfo is None:
                        # Se não tem timezone, assumir São Paulo e converter para UTC
                        datetime_sp = self.target_timezone.localize(datetime_utc)
                        datetime_utc = datetime_sp.astimezone(timezone.utc)
                    elif datetime_utc.tzinfo != timezone.utc:
                        # Se tem timezone mas não é UTC, converter
                        datetime_utc = datetime_utc.astimezone(timezone.utc)
                    
                    # Salvar o lembrete
                    self._save_reminder_to_db(
                        chat_id,
                        reminder_details["content"],
                        datetime_utc,
                        reminder_details.get("recurrence", "none"),
                        all_message_ids[-1] if all_message_ids else None
                    )
                    
                    # Adicionar confirmação do lembrete à resposta
                    datetime_local = datetime_utc.astimezone(self.target_timezone)
                    datetime_local_str = datetime_local.strftime('%d/%m/%Y às %H:%M')
                    
                    confirmation_text = f"\n\n✅ Lembrete agendado para {datetime_local_str}"
                    if reminder_details.get("recurrence", "none") != "none":
                        confirmation_text += f" (Recorrência: {reminder_details['recurrence']})"
                    
                    response_text += confirmation_text

            # Enviar resposta ao WhatsApp
            last_message_id_to_reply = all_message_ids[-1] if all_message_ids else None
            if self.send_whatsapp_message(chat_id, response_text, reply_to=last_message_id_to_reply):
                logger.info(f"Resposta enviada com sucesso para {chat_id}.")
            else:
                logger.error(f"Falha ao enviar resposta para {chat_id}.")

            # Atualizar histórico e limpar mensagens pendentes
            self.update_conversation_context(chat_id, full_user_input_text, response_text)
            self._delete_pending_messages(chat_id) # Sucesso, deleta as pendentes

        except Exception as e:
            logger.error(f"ERRO CRÍTICO ao processar mensagens para {chat_id}: {e}", exc_info=True)
            # Em caso de erro crítico, resetar 'processing' para permitir nova tentativa.
            try:
                doc_ref.update({'processing': False})
            except Exception as e_update_fail:
                logger.error(f"Falha ao resetar 'processing' para {chat_id} após erro: {e_update_fail}")
        finally:
            # Garantir que o summarizer seja chamado se necessário, mesmo se houver falha no processamento principal
            # (talvez não seja o melhor lugar, mas para garantir que rode)
            self._summarize_chat_history_if_needed(chat_id)


    def _check_inactive_chats(self):
        """Verifica chats inativos para reengajamento inteligente."""
        try:
            logger.info("Verificando chats inativos para reengajamento...")
            # Limite de tempo para considerar um chat inativo
            cutoff_reengagement = datetime.now(timezone.utc) - timedelta(seconds=self.REENGAGEMENT_TIMEOUT)

            # Consulta para encontrar o último timestamp por chat_id no histórico
            # Esta query pode ser complexa/ineficiente em Firestore para muitos chats.
            # Uma abordagem alternativa seria ter uma coleção 'last_activity' por chat.
            # Por simplicidade, vamos tentar buscar os chats e verificar a última mensagem.
            
            # Obter todos os chat_ids distintos da coleção conversation_contexts
            # (onde armazenamos last_updated, o que pode servir de proxy)
            contexts_ref = self.db.collection("conversation_contexts")
            # Order by last_updated and filter those older than cutoff
            query = contexts_ref.where(filter=FieldFilter("last_updated", "<", cutoff_reengagement)).stream()

            processed_chats_for_reengagement = set()

            for doc_context in query:
                chat_id = doc_context.id
                if chat_id in processed_chats_for_reengagement:
                    continue

                # Verificar se já houve reengajamento recente
                reengagement_log_ref = self.db.collection("reengagement_logs").document(chat_id)
                reengagement_log_doc = reengagement_log_ref.get()
                if reengagement_log_doc.exists:
                    last_sent_reengagement = reengagement_log_doc.get("last_sent")
                    # Não reenviar se já foi feito nas últimas N horas (ex: 23 horas para evitar spam diário)
                    if (datetime.now(timezone.utc) - last_sent_reengagement) < timedelta(hours=23):
                        logger.debug(f"Reengajamento recente para {chat_id}, pulando.")
                        continue
                
                logger.info(f"Chat {chat_id} inativo. Tentando reengajamento inteligente.")
                self._send_reengagement_message(chat_id)
                processed_chats_for_reengagement.add(chat_id)
                time.sleep(1) # Pequeno delay para não sobrecarregar APIs

        except Exception as e:
            logger.error(f"Erro ao verificar chats inativos: {e}", exc_info=True)

    def _send_reengagement_message(self, chat_id: str):
        """Envia mensagem de reengajamento gerada pelo Gemini com base no histórico."""
        try:
            
            # Obter resumo (se houver) e histórico recente
            summary_ref = self.db.collection("conversation_summaries").document(chat_id)
            summary_doc = summary_ref.get()
            summary_text = summary_doc.get("summary") if summary_doc.exists else ""

            history_list = self._get_conversation_history(chat_id, limit=25) # Últimas 10 trocas
            
            history_parts_reengagement = []
            for msg in history_list:
                role = "Usuário" if not msg.get('is_bot', False) else "Assistente"
                history_parts_reengagement.append(f"{role}: {msg['message_text']}")
            history_str_reengagement = "\n".join(history_parts_reengagement)

            reengagement_instruction = (
                "O usuário deste chat não interage há algum tempo (cerca de 36 horas ou mais). "
                "Seu objetivo é gerar uma mensagem de reengajamento curta, amigável e personalizada, focando em despertar o interesse do usuário e incentivá-lo a retomar a conversa. "
                "Siga as seguintes diretrizes, priorizando as opções de reengajamento mais relevantes e interessantes:"
                "\n\n"
                "1. **Análise do histórico:** Primeiramente, examine o histórico de conversa do usuário e/ou o resumo da conversa (se disponível). "
                "   - **Tópico recente:** Se houver um tópico recente claramente definido, comece por perguntar se ele ainda precisa de ajuda ou se gostaria de continuar a discussão sobre esse assunto. "
                "   - **Interesses inferidos:** Tente identificar interesses ou temas recorrentes no histórico de conversa. Use esses insights para sugerir tópicos relacionados ou informações adicionais que possam ser do seu interesse."
                "\n\n"
                "2. **Pesquisa web para assuntos relacionados:** Se o histórico de conversa permitir a identificação de tópicos ou interesses, faça uma pesquisa web para encontrar notícias recentes, curiosidades ou desenvolvimentos relevantes sobre esses temas. "
                "   - Apresente uma breve e intrigante informação encontrada, convidando o usuário a explorar mais."
                "\n\n"
                "3. **Criatividade e assuntos aleatórios:** Se não houver histórico de conversa substancial ou se os interesses do usuário não forem claros, use sua criatividade para puxar um assunto aleatório, mas que seja potencialmente interessante. "
                "   - Você pode: "
                "     - Mencionar uma notícia popular ou um evento atual (se relevante e não sensível). "
                "     - Fazer uma pergunta curiosa sobre um tema geral (tecnologia, ciência, cultura, etc.). "
                "     - Sugerir uma nova funcionalidade ou capacidade do Gemini (se aplicável). "
                "\n\n"
                "4. **Abertura geral:** Se as opções acima não se aplicarem ou não forem eficazes, ou se você precisar de uma alternativa mais genérica, envie uma saudação amigável perguntando simplesmente como pode ser útil hoje ou como o usuário está. "
                "\n\n"
                "5. **Tom e concisão:** Mantenha a mensagem concisa, natural e convidativa. Evite parecer robótico ou excessivamente formal. O objetivo é reaquecer a interação de forma orgânica. "
                "   - Exemplo de saudação amigável: 'Oi! Já faz um tempinho que não conversamos. Como posso te ajudar hoje?'"
            )

            context_for_reengagement_prompt = ""
            if summary_text:
                context_for_reengagement_prompt += f"Resumo da conversa anterior:\n{summary_text}\n\n"
            if history_str_reengagement:
                context_for_reengagement_prompt += f"Histórico recente:\n{history_str_reengagement}\n\n"
            
            if not context_for_reengagement_prompt: # Sem histórico ou resumo
                 context_for_reengagement_prompt = "Não há histórico de conversa anterior com este usuário.\n"

            full_reengagement_prompt = reengagement_instruction + context_for_reengagement_prompt + "\nMensagem de reengajamento gerada:"

            logger.info(f"Gerando mensagem de reengajamento para {chat_id} com prompt: {full_reengagement_prompt[:300]}...")

            google_search_tool = Tool(google_search=GoogleSearch())

            reengagement_response = self.client.models.generate_content(
                model=self.gemini_model_name,
                contents=full_reengagement_prompt,
                config=GenerateContentConfig(
                    tools=[google_search_tool],
                    response_modalities=["TEXT"],
                    system_instruction=self.gemini_context,
                    temperature=0.85
                )
            )
            reengagement_message_text = reengagement_response.text.strip()

            if not reengagement_message_text or len(reengagement_message_text) < 10: # Validação mínima
                logger.warning(f"Mensagem de reengajamento gerada para {chat_id} é muito curta ou vazia: '{reengagement_message_text}'. Usando fallback.")
                import random
                reengagement_message_text = random.choice(self.FALLBACK_REENGAGEMENT_MESSAGES)

            # Envia a mensagem
            if self.send_whatsapp_message(chat_id, reengagement_message_text, reply_to=None):
                # Registra o envio bem-sucedido
                reengagement_log_ref = self.db.collection("reengagement_logs").document(chat_id)
                reengagement_log_ref.set({
                    "last_sent": firestore.SERVER_TIMESTAMP,
                    "message_sent": reengagement_message_text,
                    "prompt_used_hash": hash(full_reengagement_prompt) # Para debug, se necessário
                }, merge=True)
                logger.info(f"Mensagem de reengajamento inteligente enviada para {chat_id}: {reengagement_message_text}")
                # Adiciona ao histórico do chat que o bot tentou reengajar
                self._save_conversation_history(chat_id, reengagement_message_text, True)
            else:
                logger.error(f"Falha ao enviar mensagem de reengajamento para {chat_id}.")

        except Exception as e:
            logger.error(f"Erro ao gerar/enviar mensagem de reengajamento para {chat_id}: {e}", exc_info=True)

    def generate_gemini_response(self, current_input_text: str, chat_id: str, current_message_timestamp: datetime, from_name: Optional[str] = None) -> str:
        """Gera resposta do Gemini considerando o contexto completo e usando Google Search tool."""
        try:
            # current_input_text é o texto já processado (incluindo descrições de mídia)
            full_prompt_with_history = self.build_context_prompt(chat_id, current_input_text, current_message_timestamp, from_name) # Passar from_name
            
            google_search_tool = Tool(google_search=GoogleSearch())

            response = self.client.models.generate_content(
                model=self.gemini_model_name,
                contents=[full_prompt_with_history],
                config=GenerateContentConfig(
                    tools=[google_search_tool],
                    response_modalities=["TEXT"],
                    system_instruction=self.gemini_context,
                    temperature=0.55
                )
            )
            
            # Para extrair o texto da resposta quando tools são usadas:
            # A API pode retornar partes diferentes. Precisamos do texto gerado.
            generated_text = ""
            if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
                for part in response.candidates[0].content.parts:
                    if hasattr(part, 'text'):
                        generated_text += part.text
            
            # Log se houve uso de ferramenta (grounding)
            if response.candidates and response.candidates[0].grounding_metadata:
                 search_entry = response.candidates[0].grounding_metadata.search_entry_point
                 if search_entry:
                      logger.info(f"Gemini usou Google Search.")


            return generated_text.strip() if generated_text else "Desculpe, não consegui processar sua solicitação no momento."

        except Exception as e:
            logger.error(f"Erro na chamada ao Gemini para chat {chat_id}: {e}", exc_info=True)
            return "Desculpe, ocorreu um erro ao tentar gerar uma resposta. Por favor, tente novamente."

    def send_whatsapp_message(self, chat_id: str, text: str, reply_to: Optional[str]) -> bool:
        """Envia mensagem formatada para o WhatsApp"""
        if not text or not chat_id:
            logger.error("Dados inválidos para envio de mensagem: chat_id ou texto ausente.")
            return False

        # Limitar tamanho da mensagem se necessário (WhatsApp tem limites)
        max_len = 4096 
        if len(text) > max_len:
            logger.warning(f"Mensagem para {chat_id} excedeu {max_len} caracteres. Será truncada.")
            text = text[:max_len-3] + "..."

        payload = {
            "to": chat_id,
            "body": text,
        }
        if reply_to:
            payload["reply"] = reply_to # Whapi usa "reply" para o ID da mensagem a ser respondida

        try:
            response = requests.post(
                "https://gate.whapi.cloud/messages/text",
                headers={
                    "Authorization": f"Bearer {self.whapi_api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json" # Adicionado por boa prática
                },
                json=payload,
                timeout=20 # Timeout aumentado um pouco
            )

            logger.info(f"Resposta WHAPI (Status {response.status_code}): {response.text}")
            response.raise_for_status() # Levanta erro para status >= 400
            return True # Whapi costuma retornar 200 ou 201 para sucesso

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"Erro HTTP ao enviar mensagem para {chat_id}: {http_err} - {response.text}")
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Erro de requisição ao enviar mensagem para {chat_id}: {req_err}")
        except Exception as e:
            logger.error(f"Falha inesperada no envio da mensagem para {chat_id}: {e}", exc_info=True)
        
        return False

    def _summarize_chat_history_if_needed(self, chat_id: str):
        """Verifica se é hora de resumir o histórico e o faz."""
        try:
            # Contar mensagens não resumidas
            query = (
                self.db.collection("conversation_history")
                .where(filter=FieldFilter("chat_id", "==", chat_id))
                .where(filter=FieldFilter("summarized", "==", False))
            )
            # Contar documentos pode ser caro. Uma alternativa é buscar com limit.
            # Se o número de documentos retornados atingir o limite, então resumir.
            docs_to_check = list(query.limit(26).stream()) # Um a mais que o limite para saber se passou

            if len(docs_to_check) < 25: # Limite para resumir
                return
            
            # Pegar as mensagens para resumir (as 100 mais antigas não resumidas)
            query_summarize = (
                self.db.collection("conversation_history")
                .where(filter=FieldFilter("chat_id", "==", chat_id))
                .where(filter=FieldFilter("summarized", "==", False))
                .order_by("timestamp", direction=firestore.Query.ASCENDING) # Mais antigas primeiro
                .limit(25) # Resumir em lotes
            )
            docs_to_summarize = list(query_summarize.stream())

            if not docs_to_summarize:
                return

            logger.info(f"Gerando resumo para {len(docs_to_summarize)} mensagens do chat {chat_id}")
            
            # Concatenar mensagens para o prompt de resumo
            # Adicionar papel (Usuário/Assistente) para clareza no resumo
            message_texts_for_summary = []
            for doc in docs_to_summarize:
                data = doc.to_dict()
                role = "Usuário" if not data.get("is_bot") else "Assistente"
                message_texts_for_summary.append(f"{role}: {data.get('message_text', '')}")
            
            full_text_for_summary = "\n".join(message_texts_for_summary)

            summary_prompt = (
                "Você é um assistente encarregado de resumir conversas. Abaixo está um trecho das interações do usuário. "
                "Seu objetivo é criar um resumo conciso que capture os pontos principais, decisões tomadas, informações importantes compartilhadas (nomes, locais, datas, preferências, problemas, soluções), "
                "e o sentimento geral ou intenção da conversa. O resumo será usado para dar contexto a futuras interações, use o minimo possível de palavras e seja vem objetivo.\n\n"
                "CONVERSA:\n"
                f"{full_text_for_summary}\n\n"
            )
            
            # Gerar resumo com Gemini (sem tools aqui)
            response = self.client.models.generate_content(
            model=self.gemini_model_name,
            contents=summary_prompt,
            config=self.model_config
        )
            summary = response.text.strip()

            if not summary:
                logger.warning(f"Resumo gerado para {chat_id} está vazio. Não será salvo.")
                return

            # Obter resumo anterior, se existir, para concatenar
            summary_ref = self.db.collection("conversation_summaries").document(chat_id)
            summary_doc = summary_ref.get()
            previous_summary = summary_doc.get("summary") if summary_doc.exists else ""
            
            # Novo resumo = resumo anterior + novo resumo (ou lógica mais inteligente de merge)
            # Por simplicidade, vamos apenas adicionar o novo. Para um sistema robusto, um resumo do resumo pode ser melhor.
            # Ou, o Gemini poderia receber o resumo anterior e o novo trecho para gerar um resumo atualizado.
            # Por ora:
            updated_summary = f"{previous_summary}\n\n[Novo trecho resumido em {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}]:\n{summary}".strip()


            summary_ref.set({
                "summary": updated_summary,
                "last_updated": firestore.SERVER_TIMESTAMP,
                "last_chunk_timestamp": docs_to_summarize[-1].get("timestamp") # Timestamp da última msg resumida neste lote
            }, merge=True)

            # Marcar as mensagens como resumidas
            batch = self.db.batch()
            for doc_to_mark in docs_to_summarize:
                batch.update(doc_to_mark.reference, {"summarized": True})
            batch.commit()
            logger.info(f"{len(docs_to_summarize)} mensagens marcadas como resumidas para o chat {chat_id}. Novo resumo salvo.")

        except Exception as e:
            logger.error(f"Erro ao gerar/salvar resumo para o chat {chat_id}: {e}", exc_info=True)


    def run(self):
        """Inicia verificação periódica de mensagens pendentes e outras tarefas de manutenção."""
        try:
            logger.info("Iniciando loop principal de verificação do bot...")
            last_reengagement_check = datetime.now(timezone.utc)
            last_reengagement_check = datetime.now(timezone.utc)
            last_reminder_check = datetime.now(timezone.utc) - timedelta(seconds=self.REMINDER_CHECK_INTERVAL_SECONDS) # Check soon after start
            last_pending_reminder_cleanup = datetime.now(timezone.utc)
            # last_summarization_check = datetime.now(timezone.utc) # _summarize_chat_history_if_needed é chamado após cada processamento

            while True:
                try:
                    now = datetime.now(timezone.utc)
                    
                    # 1. Verificar e processar chats com mensagens pendentes que atingiram timeout
                    self._check_all_pending_chats_for_processing()

                    # 2. Verificar chats inativos para reengajamento (ex: a cada hora)
                    if (now - last_reengagement_check) >= timedelta(hours=1): # Ajuste o intervalo conforme necessidade
                        self._check_inactive_chats()
                        last_reengagement_check = now
                    
                    # 3. Verificar e enviar lembretes devidos
                    if (now - last_reminder_check) >= timedelta(seconds=self.REMINDER_CHECK_INTERVAL_SECONDS):
                        self._check_and_send_due_reminders()
                        last_reminder_check = now

                    # 4. Limpar sessões de criação de lembretes pendentes e expiradas
                    if (now - last_pending_reminder_cleanup) >= timedelta(seconds=self.REMINDER_SESSION_TIMEOUT_SECONDS): # Check as often as timeout
                        self._cleanup_stale_pending_reminder_sessions()
                        last_pending_reminder_cleanup = now
                    
                    # 5. Outras tarefas de manutenção (resumo é chamado no _process_pending_messages)

                except Exception as e:
                    logger.error(f"Erro no ciclo principal de verificação do bot: {e}", exc_info=True)

                time.sleep(self.PENDING_CHECK_INTERVAL) # Intervalo base do loop

        except KeyboardInterrupt:
            logger.info("Bot encerrado manualmente.")
        except Exception as e:
            logger.error(f"Erro fatal no loop principal do bot: {e}", exc_info=True)

    def _check_all_pending_chats_for_processing(self):
        """Verifica todos os chats com mensagens pendentes e cujo timeout foi atingido."""
        try:
            now = datetime.now(timezone.utc)
            # O cutoff é relativo ao 'last_update' do documento de pending_messages.
            # Se last_update for muito antigo, significa que as mensagens estão esperando há muito tempo.
            cutoff_for_pending = now - timedelta(seconds=self.pending_timeout)

            # logger.debug(f"Verificando chats pendentes (last_update < {cutoff_for_pending}) e não processando...")

            query = (
                self.db.collection("pending_messages")
                .where(filter=FieldFilter("processing", "==", False)) # Apenas os não marcados como 'processing'
                .where(filter=FieldFilter("last_update", "<=", cutoff_for_pending)) # Que atingiram o timeout
            )
            
            # Limitar o número de chats processados por ciclo para evitar sobrecarga, se necessário
            # query = query.limit(10) 
            
            docs = query.stream()
            chats_to_process_ids = [doc.id for doc in docs]

            if chats_to_process_ids:
                logger.info(f"Chats pendentes encontrados para processamento: {len(chats_to_process_ids)}. IDs: {chats_to_process_ids}")
                for chat_id in chats_to_process_ids:
                    # _check_pending_messages irá verificar novamente e marcar 'processing' com transação
                    self._check_pending_messages(chat_id) 
                    time.sleep(0.5) # Pequeno delay entre processamento de chats diferentes
            # else:
                # logger.debug("Nenhum chat pendente atingiu o timeout de processamento neste ciclo.")

        except Exception as e:
            logger.error(f"Erro na verificação de todos os chats pendentes: {e}", exc_info=True)

# Inicialização do Bot e Thread
bot = WhatsAppGeminiBot()

# Movido para dentro do if __name__ == "__main__": para execução controlada
# from threading import Thread
# bot_thread = Thread(target=bot.run, daemon=True)
# bot_thread.start()

if __name__ == "__main__":
    logger.info("Iniciando o bot WhatsAppGeminiBot em uma thread separada...")
    from threading import Thread
    bot_thread = Thread(target=bot.run, name="BotWorkerThread", daemon=True)
    bot_thread.start()
    
    # Este join() manteria o script principal rodando até a thread do bot terminar,
    # o que só acontece com KeyboardInterrupt ou erro fatal na thread.
    # Para um servidor que também roda Flask (webhook.py), o Flask app.run() seria o bloqueador principal.
    # Se este main.py é só para o worker do bot, o join é apropriado.
    try:
        while bot_thread.is_alive():
            bot_thread.join(timeout=1.0) # Permite checar por interrupção
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt recebido no script principal. Encerrando o bot...")
    except Exception as e:
        logger.error(f"Erro fatal no script principal ao aguardar o bot: {e}", exc_info=True)
    finally:
        logger.info("Script principal do bot finalizado.")
