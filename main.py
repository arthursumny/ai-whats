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


# Carrega variáveis do .env
load_dotenv()

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
    PENDING_CHECK_INTERVAL = 5
    REENGAGEMENT_TIMEOUT = (60 * 60 * 24 * 3)  # 3 dias em segundos
    # REENGAGEMENT_MESSAGES não será mais usado para a lógica principal,
    # mas pode ser um fallback se a geração do Gemini falhar.
    FALLBACK_REENGAGEMENT_MESSAGES = [
        "Oi! Está tudo bem por aí? Posso ajudar com algo?",
        "Oi! Como posso ajudar você hoje?",
    ]

    # Reminder feature constants
    REMINDER_REQUEST_KEYWORDS_REGEX = r"(me\s+lembre|lembre-me|criar\s+lembrete|novo\s+lembrete|lembrete\s+para|agendar\s+lembrete|anotar\s+lembrete|nao\s+me\s+deixe\s+esquecer|nao\s+esquecer\s+de)"
    REMINDER_STATE_AWAITING_CONTENT = "awaiting_content"
    REMINDER_STATE_AWAITING_DATETIME = "awaiting_datetime"
    REMINDER_STATE_AWAITING_RECURRENCE = "awaiting_recurrence"
    REMINDER_SESSION_TIMEOUT_SECONDS = 300  # 5 minutes for pending reminder session
    REMINDER_CHECK_INTERVAL_SECONDS = 60 # Check for due reminders every 60 seconds

    PORTUGUESE_DAYS_FOR_PARSING = {
        "segunda": "monday", "terça": "tuesday", "quarta": "wednesday",
        "quinta": "thursday", "sexta": "friday", "sábado": "saturday", "domingo": "sunday",
        "segunda-feira": "monday", "terça-feira": "tuesday", "quarta-feira": "wednesday",
        "quinta-feira": "thursday", "sexta-feira": "friday"
    }
    RECURRENCE_KEYWORDS = {
        "diariamente": "daily", "todo dia": "daily", "todos os dias": "daily",
        "semanalmente": "weekly", "toda semana": "weekly", "todas as semanas": "weekly",
        "mensalmente": "monthly", "todo mes": "monthly", "todos os meses": "monthly", # "mes" without accent for easier regex
        "anualmente": "yearly", "todo ano": "yearly", "todos os anos": "yearly"
    }

    def __init__(self):
        self.reload_env()
        self.db = firestore.Client(project="voola-ai") # Seu projeto
        self.pending_timeout = 15  # Timeout para mensagens pendentes (em segundos)
        
        if not all([self.whapi_api_key, self.gemini_api_key]):
            raise ValueError("Chaves API não configuradas no .env")
        
        self.setup_apis()
        self.pending_reminder_sessions: Dict[str, Dict[str, Any]] = {}

    def _get_pending_messages(self, chat_id: str) -> Dict[str, Any]:
        """Obtém mensagens pendentes para um chat"""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        return {}
    
    def _save_pending_message(self, chat_id: str, message_payload: Dict[str, Any]):
        """
        Armazena mensagem temporariamente com timestamp.
        message_payload deve conter: type, content, original_caption, mimetype, timestamp, message_id
        """
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        # Usar transação para garantir consistência ao adicionar mensagens
        @firestore.transactional
        def update_in_transaction(transaction, doc_ref, new_message):
            snapshot = doc_ref.get(transaction=transaction)
            existing_data = snapshot.to_dict() if snapshot.exists else {}
            
            messages = existing_data.get('messages', [])
            messages.append(new_message)

            transaction.set(doc_ref, {
                'messages': messages,
                'last_update': datetime.now(timezone.utc), # Sempre atualiza o timestamp do documento
                'processing': existing_data.get('processing', False) 
            }, merge=True) # Merge para não sobrescrever 'processing' se já estiver lá

        update_in_transaction(self.db.transaction(), doc_ref, message_payload)
        logger.info(f"Mensagem pendente salva para {chat_id}: {message_payload.get('type')}")


    def _delete_pending_messages(self, chat_id: str):
        """Remove mensagens processadas"""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        doc_ref.delete()
        logger.info(f"Mensagens pendentes removidas para {chat_id}")

    def _message_exists(self, message_id: str) -> bool:
        """Verifica se a mensagem já foi processada (Firestore)"""
        doc_ref = self.db.collection("processed_messages").document(message_id)
        return doc_ref.get().exists

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

            logger.info(f"Configuração do Gemini com modelo {self.gemini_model_name} concluída.")
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

    def build_context_prompt(self, chat_id: str, current_prompt_text: str, current_message_timestamp: datetime) -> str:
        """Constrói o prompt com histórico formatado corretamente, incluindo o resumo."""
        try:
            summary_ref = self.db.collection("conversation_summaries").document(chat_id)
            summary_doc = summary_ref.get()
            summary = summary_doc.get("summary") if summary_doc.exists else ""

            history = self._get_conversation_history(chat_id, limit=100) # Limite menor para prompt

            current_timestamp_iso = current_message_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')

            if not history and not summary:
                return f"Usuário: {current_prompt_text}" # Adiciona prefixo Usuário

            # Ordenar cronologicamente já é feito por _get_conversation_history
            context_parts = []
            for msg in history:
                role = "Usuário" if not msg.get('is_bot', False) else "Assistente"
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
                final_prompt.append(f"### Resumo da conversa anterior ###\n{summary}\n")
            if context_str: 
                final_prompt.append(f"### Histórico recente da conversa (com timestamps UTC) ###\n{context_str}\n")
            
            final_prompt.append(
                "### Nova interação, responda a esta nova interação. ###\n"
                f"A mensagem atual do usuário foi recebida em {current_timestamp_iso} (UTC).\n"
                "Considere os timestamps das mensagens do histórico e da mensagem atual. "
                "Se uma mensagem do histórico for significativamente antiga em relação à mensagem atual, "
                "avalie cuidadosamente se o tópico ainda é relevante e se faz sentido continuar ou referenciar essa conversa antiga. "
                "Priorize a relevância para a interação atual. "
                "Use o histórico e o resumo acima como contexto apenas se forem pertinentes para a nova interação."
            )
            final_prompt.append(f"Usuário (em {current_timestamp_iso}): {current_prompt_text}")
            
            return "\n".join(final_prompt)

        except Exception as e:
            logger.error(f"Erro ao construir contexto para o chat {chat_id}: {e}")
            return f"Usuário: {current_prompt_text}" # Fallback simples

    def test_whapi_connection(self):
        try:
            response = requests.get(
                "https://gate.whapi.cloud/settings", # Removida barra final se não necessária
                headers={"Authorization": f"Bearer {self.whapi_api_key}"},
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"Conexão com Whapi.cloud verificada com sucesso: {response.json()}")
            return True
        except Exception as e:
            logger.error(f"Falha na conexão com Whapi.cloud: {e}")
            raise

    def process_whatsapp_message(self, message: Dict[str, Any]) -> None:
        logger.info(f"Raw mensagem recebida: {message}")

        message_id = message.get('id')
        if not message_id:
            logger.warning("Mensagem sem ID recebida, ignorando.")
            return

        if self._message_exists(message_id) and not self.pending_reminder_sessions.get(message.get('chat_id')):
            logger.info(f"Mensagem {message_id} já processada e não há sessão de lembrete pendente, ignorando.")
            return

        chat_id = message.get('chat_id')
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

        
        # --- Reminder Flow Logic ---
        if chat_id in self.pending_reminder_sessions:
            self._save_message(message_id, chat_id, text_body, from_name, "text") # Log user's reply
            self._save_conversation_history(chat_id, text_body, False)
            self._handle_pending_reminder_interaction(chat_id, text_body, message_id)
            return # Reminder flow handles its own response

        if self._is_reminder_request(text_body):
            self._save_message(message_id, chat_id, text_body, from_name, "text") # Log user's request
            self._save_conversation_history(chat_id, text_body, False)
            self._initiate_reminder_creation(chat_id, text_body, message_id)
            return # Reminder flow handles its own response
        # --- End Reminder Flow Logic ---

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
            logger.info(f"Imagem recebida: {media_url}")
        elif msg_type_whapi == 'video' and 'video' in message:
            media_url = message['video'].get('link')
            logger.info(f"Imagem recebida: {media_url}")
        elif msg_type_whapi == 'document' and 'document' in message:
            media_url = message['document'].get('link')
            logger.info(f"Imagem recebida: {media_url}")
        elif msg_type_whapi == 'voice' and 'voice' in message:
            media_url = message['voice'].get('link')
            logger.info(f"Imagem recebida: {media_url}")

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

        self._save_pending_message(chat_id, pending_payload)
        logger.info(f"Mensagem de {from_name} ({chat_id}) adicionada à fila pendente. Tipo: {processed_type_internal}.")

    # --- Methods for Reminder Feature ---
    def _is_reminder_request(self, text: str) -> bool:
        """Checks if the text contains keywords indicating a reminder request."""
        if not text:
            return False
        return bool(re.search(self.REMINDER_REQUEST_KEYWORDS_REGEX, text, re.IGNORECASE))

    def _clean_text_for_parsing(self, text: str) -> str:
        """Prepares text for date/time parsing by translating Portuguese day names."""
        processed_text = text.lower()
        for pt_day, en_day in self.PORTUGUESE_DAYS_FOR_PARSING.items():
            processed_text = re.sub(r'\b' + pt_day + r'\b', en_day, processed_text)
        
        # Handle "hoje", "amanhã", "depois de amanha" by replacing with parsable dates
        now = datetime.now(timezone.utc)
        processed_text = re.sub(r'\bhoje\b', now.strftime('%Y-%m-%d'), processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'\bamanhã\b', (now + timedelta(days=1)).strftime('%Y-%m-%d'), processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'\bdepois de amanhã\b', (now + timedelta(days=2)).strftime('%Y-%m-%d'), processed_text, flags=re.IGNORECASE)
        
        # "próxima segunda" -> "next monday"
        processed_text = re.sub(r'próxima\s+', 'next ', processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'próximo\s+', 'next ', processed_text, flags=re.IGNORECASE)
        return processed_text

    def _extract_reminder_details_from_text(self, text: str, chat_id: str) -> Dict[str, Any]:
        """
        Extracts content, datetime, and recurrence from text.
        This is a simplified version and can be improved with more robust regex.
        """
        details = {
            "content": None,
            "datetime_obj": None,
            "recurrence": "none", # Default if not specified
            "original_datetime_str": None # Store the part of text identified as datetime
        }
        
        # Remove reminder keywords to isolate payload
        payload_text = re.sub(self.REMINDER_REQUEST_KEYWORDS_REGEX, "", text, flags=re.IGNORECASE).strip()
        # Also remove common prepositions that might precede the actual content after keywords
        payload_text = re.sub(r"^(de|para|que)\s+", "", payload_text, flags=re.IGNORECASE).strip()

        if not payload_text:
            return details # Not enough info

        text_to_parse_for_datetime_and_content = payload_text
        
        # 1. Extract Recurrence
        # Iterate and remove recurrence phrases first, as they are more distinct
        # Store the longest recurrence phrase found
        found_recurrence_phrase = ""
        for phrase, key in self.RECURRENCE_KEYWORDS.items():
            match = re.search(r'\b' + phrase + r'\b', text_to_parse_for_datetime_and_content, re.IGNORECASE)
            if match:
                if len(phrase) > len(found_recurrence_phrase): # Prioritize longer matches
                    found_recurrence_phrase = match.group(0)
                    details["recurrence"] = key
        
        if found_recurrence_phrase:
            text_to_parse_for_datetime_and_content = text_to_parse_for_datetime_and_content.replace(found_recurrence_phrase, "").strip()

        # 2. Extract Datetime
        # Use dateutil.parser.parse with fuzzy_with_tokens to separate date/time from other text
        cleaned_for_datetime_parsing = self._clean_text_for_parsing(text_to_parse_for_datetime_and_content)
        
        # Try to parse, assuming the user is in the bot's system timezone (or we default to UTC interpretation)
        # For more accuracy, one might need to know the user's timezone.
        # `dayfirst=True` helps with DD/MM/YYYY formats common in Brazil.
        try:
            # fuzzy_with_tokens returns (datetime_obj, tuple_of_non_datetime_tokens)
            parsed_datetime, non_datetime_tokens = dateutil_parser.parse(cleaned_for_datetime_parsing, fuzzy_with_tokens=True, dayfirst=True)
            
            # Ensure the parsed datetime is timezone-aware (UTC)
            if parsed_datetime.tzinfo is None:
                # This assumes the time given is local to where the bot is running, then converts to UTC.
                # A better approach might involve asking user for timezone or using a default.
                # For now, let's assume it's intended as UTC if no TZ info, or make it local then UTC.
                # Let's assume the parsed time is in the system's local timezone and convert to UTC.
                # local_tz = datetime.now(timezone.utc).astimezone().tzinfo # Get local system timezone
                # parsed_datetime = parsed_datetime.replace(tzinfo=local_tz)
                # details["datetime_obj"] = parsed_datetime.astimezone(timezone.utc)
                
                # Simpler: if naive, assume it's for "today" in UTC context or a future date.
                # dateutil often defaults to current day if only time is given.
                # If it's in the past (e.g. parsed "10:00" as today 10:00 but it's already 11:00), advance it.
                now_utc = datetime.now(timezone.utc)
                parsed_datetime_utc = parsed_datetime.replace(tzinfo=timezone.utc) # Tentatively UTC

                if parsed_datetime_utc < now_utc and parsed_datetime.time() == parsed_datetime_utc.time(): # if only time was given and it's past
                     parsed_datetime_utc += timedelta(days=1)
                details["datetime_obj"] = parsed_datetime_utc

            else: # Already timezone-aware
                details["datetime_obj"] = parsed_datetime.astimezone(timezone.utc)

            # Reconstruct what was likely the datetime string from the original text
            # This is tricky. The non_datetime_tokens are parts *not* used.
            # So, the original text minus these tokens is roughly the datetime string.
            temp_content_parts = [token.strip() for token in non_datetime_tokens if token.strip()]
            details["content"] = " ".join(temp_content_parts).strip()
            
            # Try to find the original datetime string (this is an approximation)
            # We need to find what part of `text_to_parse_for_datetime_and_content` became `details["datetime_obj"]`
            # This is complex. For now, we'll just use the successfully parsed object.
            # details["original_datetime_str"] = "extracted datetime part" # Placeholder

        except (ValueError, TypeError) as e:
            logger.info(f"Could not parse datetime from '{cleaned_for_datetime_parsing}': {e}")
            details["content"] = text_to_parse_for_datetime_and_content # If no date, all remaining is content
        
        # If content is empty after extraction, it means the whole payload was date/recurrence
        if not details["content"] and payload_text and (details["datetime_obj"] or details["recurrence"] != "none"):
             details["content"] = None # Explicitly mark as not found if only date/recurrence was in payload

        # Final cleanup of content: remove reminder keywords again if they somehow survived
        if details["content"]:
            details["content"] = re.sub(self.REMINDER_REQUEST_KEYWORDS_REGEX, "", details["content"], flags=re.IGNORECASE).strip()
            details["content"] = re.sub(r"^(de|para|que)\s+", "", details["content"], flags=re.IGNORECASE).strip()
            if not details["content"]: details["content"] = None

        return details

    def _initiate_reminder_creation(self, chat_id: str, text: str, message_id: str):
        """Starts the process of creating a new reminder."""
        logger.info(f"Initiating reminder creation for chat {chat_id} from text: {text}")
        
        # Clean up any previous stale session for this chat_id
        if chat_id in self.pending_reminder_sessions:
            del self.pending_reminder_sessions[chat_id]

        extracted_details = self._extract_reminder_details_from_text(text, chat_id)
        
        content = extracted_details.get("content")
        datetime_obj = extracted_details.get("datetime_obj")
        recurrence = extracted_details.get("recurrence", "none")

        session_data = {
            "state": "",
            "content": content,
            "datetime_obj": datetime_obj,
            "recurrence": recurrence,
            "original_message_id": message_id,
            "last_interaction": datetime.now(timezone.utc)
        }

        if not content:
            session_data["state"] = self.REMINDER_STATE_AWAITING_CONTENT
        elif not datetime_obj:
            session_data["state"] = self.REMINDER_STATE_AWAITING_DATETIME
        # Recurrence has a default ("none"), so we only ask if we want to confirm or change.
        # For simplicity, we'll assume "none" if not specified and not ask unless we enhance this later.
        # else if recurrence == "none": # Could ask "Deseja que este lembrete se repita?"
        # session_data["state"] = self.REMINDER_STATE_AWAITING_RECURRENCE 

        if session_data["state"]:
            self.pending_reminder_sessions[chat_id] = session_data
            self._ask_for_missing_reminder_info(chat_id, session_data)
        else:
            # All details found
            self._save_reminder_to_db(chat_id, content, datetime_obj, recurrence, message_id)
            response_text = f"Lembrete agendado para {datetime_obj.strftime('%d/%m/%Y às %H:%M')} (UTC): {content}"
            if recurrence != "none":
                response_text += f" (Recorrência: {recurrence})"
            self.send_whatsapp_message(chat_id, response_text, reply_to=message_id)
            self._save_conversation_history(chat_id, response_text, True)


    def _handle_pending_reminder_interaction(self, chat_id: str, text: str, message_id: str):
        """Handles user's response when the bot is waiting for more reminder info."""
        if chat_id not in self.pending_reminder_sessions:
            # Should not happen if called correctly
            logger.warning(f"No pending reminder session for {chat_id} in _handle_pending_reminder_interaction")
            # Fallback to standard processing if something went wrong
            # self.process_whatsapp_message(message) # This would cause a loop.
            # Instead, just log and maybe send a generic error or ignore.
            return

        session = self.pending_reminder_sessions[chat_id]
        session["last_interaction"] = datetime.now(timezone.utc) # Update interaction time

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
                session["state"] = "" # Mark as filled
            else: # Empty content
                self.send_whatsapp_message(chat_id, "O conteúdo do lembrete não pode ser vazio. Por favor, me diga o que devo lembrar.", reply_to=message_id)
                self._save_conversation_history(chat_id, "O conteúdo do lembrete não pode ser vazio. Por favor, me diga o que devo lembrar.", True)
                return


        elif current_state == self.REMINDER_STATE_AWAITING_DATETIME:
            try:
                cleaned_text = self._clean_text_for_parsing(text)
                # We expect the user to provide only the date/time here
                parsed_dt, _ = dateutil_parser.parse(cleaned_text, fuzzy_with_tokens=False, dayfirst=True) # Not fuzzy here
                
                if parsed_dt.tzinfo is None:
                    now_utc = datetime.now(timezone.utc)
                    parsed_dt_utc = parsed_dt.replace(tzinfo=timezone.utc)
                    if parsed_dt_utc < now_utc and parsed_dt.time() == parsed_dt_utc.time():
                        parsed_dt_utc += timedelta(days=1)
                    session["datetime_obj"] = parsed_dt_utc
                else:
                    session["datetime_obj"] = parsed_dt.astimezone(timezone.utc)
                
                session["state"] = "" # Mark as filled
            except (ValueError, TypeError) as e:
                logger.info(f"Could not parse datetime from user input '{text}': {e}")
                response_text = "Não consegui entender a data/hora. Por favor, tente de novo (ex: amanhã às 14:30, 25/12 09:00, hoje 18h)."
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
            self._save_reminder_to_db(
                chat_id, 
                session["content"], 
                session["datetime_obj"], 
                session.get("recurrence", "none"), 
                session["original_message_id"]
            )
            dt_obj = session["datetime_obj"]
            response_text = f"Lembrete agendado para {dt_obj.strftime('%d/%m/%Y às %H:%M')} (UTC): {session['content']}"
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


    def _save_reminder_to_db(self, chat_id: str, content: str, reminder_time_utc: datetime, recurrence: str, original_message_id: str):
        """Saves the complete reminder to Firestore."""
        try:
            doc_ref = self.db.collection("reminders").document() # Auto-generate ID
            doc_ref.set({
                "chat_id": chat_id,
                "content": content,
                "reminder_time_utc": reminder_time_utc, # Firestore will convert to its Timestamp type
                "recurrence": recurrence, # "none", "daily", "weekly", "monthly", "yearly"
                "is_active": True,
                "created_at": firestore.SERVER_TIMESTAMP,
                "last_sent_at": None, # For recurring reminders
                "original_message_id": original_message_id,
                "original_hour_utc": reminder_time_utc.hour, # Store original time components for recurrence
                "original_minute_utc": reminder_time_utc.minute,
            })
            logger.info(f"Lembrete salvo no Firestore para {chat_id}: {content} @ {reminder_time_utc}")
        except Exception as e:
            logger.error(f"Erro ao salvar lembrete para {chat_id} no Firestore: {e}", exc_info=True)
            # Inform user about failure?
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
                chat_id = reminder_data["content"]
                content = reminder_data["content"]
                recurrence = reminder_data.get("recurrence", "none")
                reminder_id = reminder_doc.id
                original_msg_id = reminder_data.get("original_message_id")
                
                # Firestore timestamps are datetime objects when read
                reminder_time_utc = reminder_data["reminder_time_utc"] 
                # Ensure it's timezone-aware (Firestore should return UTC)
                if reminder_time_utc.tzinfo is None:
                    reminder_time_utc = reminder_time_utc.replace(tzinfo=timezone.utc)

                logger.info(f"Enviando lembrete ID {reminder_id} para {chat_id}: {content}")
                
                message_to_send = f"Não esqueça de: {content}"
                
                # Send the reminder message
                success = self.send_whatsapp_message(chat_id, message_to_send, reply_to=None) # Don't reply to original msg for reminder itself

                if success:
                    self._save_conversation_history(chat_id, message_to_send, True) # Log bot's reminder
                    
                    update_data = {"last_sent_at": firestore.SERVER_TIMESTAMP}
                    if recurrence == "none":
                        update_data["is_active"] = False
                        logger.info(f"Lembrete {reminder_id} (não recorrente) marcado como inativo.")
                    else:
                        original_hour = reminder_data.get("original_hour_utc", reminder_time_utc.hour)
                        original_minute = reminder_data.get("original_minute_utc", reminder_time_utc.minute)
                        
                        next_occurrence_utc = self._get_next_occurrence(reminder_time_utc, recurrence, original_hour, original_minute)
                        if next_occurrence_utc:
                            update_data["reminder_time_utc"] = next_occurrence_utc
                            logger.info(f"Lembrete {reminder_id} (recorrência: {recurrence}) reagendado para {next_occurrence_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        else:
                            update_data["is_active"] = False # Could not calculate next, deactivate
                            logger.warning(f"Não foi possível calcular próxima ocorrência para lembrete {reminder_id}. Desativando.")
                    
                    self.db.collection("reminders").document(reminder_id).update(update_data)
                else:
                    logger.error(f"Falha ao enviar lembrete ID {reminder_id} para {chat_id}.")
                    # Optionally, implement retry logic or mark as failed_to_send

        except Exception as e:
            logger.error(f"Erro ao verificar/enviar lembretes: {e}", exc_info=True)

    def _cleanup_stale_pending_reminder_sessions(self):
        """Cleans up pending reminder sessions that have timed out."""
        now = datetime.now(timezone.utc)
        stale_sessions = []
        for chat_id, session_data in self.pending_reminder_sessions.items():
            last_interaction = session_data.get("last_interaction")
            if last_interaction and (now - last_interaction).total_seconds() > self.REMINDER_SESSION_TIMEOUT_SECONDS:
                stale_sessions.append(chat_id)
        
        for chat_id in stale_sessions:
            logger.info(f"Removendo sessão de criação de lembrete expirada para o chat {chat_id}.")
            del self.pending_reminder_sessions[chat_id]
            # Optionally notify user that the reminder creation was cancelled due to timeout
            # self.send_whatsapp_message(chat_id, "A criação do lembrete foi cancelada por inatividade.", None)

    def _check_pending_messages(self, chat_id: str):
        """Verifica se deve processar as mensagens acumuladas para um chat específico."""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        try:
            doc = doc_ref.get()
            if not doc.exists:
                return

            data = doc.to_dict()
            if data.get('processing', False):
                logger.info(f"Chat {chat_id} já está em processamento, pulando.")
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
                logger.info(f"Nenhuma mensagem na fila para {chat_id}, limpando documento pendente se existir.")
                doc_ref.delete() # Limpa se estiver vazio
                return

            # Tempo desde a última atualização (quando a última mensagem foi adicionada OU quando começou a processar)
            timeout_seconds = (now - last_update_dt).total_seconds()

            if timeout_seconds >= self.pending_timeout:
                logger.info(f"Timeout atingido para {chat_id} ({timeout_seconds}s). Marcando para processamento.")
                # Marca como processando ANTES de iniciar o processamento real
                # Usar transação para evitar condição de corrida
                @firestore.transactional
                def mark_as_processing(transaction, doc_ref_trans):
                    snapshot = doc_ref_trans.get(transaction=transaction)
                    if snapshot.exists and not snapshot.get('processing'):
                        transaction.update(doc_ref_trans, {'processing': True, 'last_update': firestore.SERVER_TIMESTAMP})
                        return True
                    return False

                if mark_as_processing(self.db.transaction(), doc_ref):
                    self._process_pending_messages(chat_id)
                else:
                    logger.info(f"Não foi possível marcar {chat_id} como processando (talvez outro worker pegou).")

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
            logger.info(f"Iniciando processamento para {chat_id}")
            
            doc = doc_ref.get() # Obter os dados mais recentes
            if not doc.exists:
                logger.warning(f"Documento de mensagens pendentes para {chat_id} não encontrado ao iniciar processamento.")
                return

            data = doc.to_dict()
            pending_msg_list = data.get('messages', [])

            if not pending_msg_list:
                logger.warning(f"Nenhuma mensagem pendente encontrada para {chat_id} ao processar.")
                self._delete_pending_messages(chat_id) # Limpa se estiver vazio
                return

            logger.info(f"Processando {len(pending_msg_list)} mensagens para {chat_id}")
            
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
                elif msg_type in ['audio', 'image', 'voice']:
                    media_url = content
                    if not mimetype:
                        # Tentar inferir mimetype da URL como último recurso (pouco confiável)
                        # Idealmente, Whapi sempre envia mimetype.
                        try:
                            file_ext = os.path.splitext(media_url.split('?')[0])[1].lower() # Remove query params
                            if file_ext == ".jpg" or file_ext == ".jpeg": mimetype = "image/jpeg"
                            elif file_ext == ".png": mimetype = "image/png"
                            elif file_ext == ".mp3": mimetype = "audio/mp3"
                            elif file_ext == ".oga": mimetype = "audio/ogg" # Comum para PTT
                            elif file_ext == ".opus": mimetype = "audio/opus"
                            elif file_ext == ".wav": mimetype = "audio/wav"
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

                    
                        prompt_for_media = "Descreva este arquivo de forma concisa e objetiva."
                        if msg_type == 'audio' or msg_type == 'voice':
                            prompt_for_media = "Transcreva este audio, exatamente como está."
                        
                        # Gerar descrição/transcrição
                        media_desc_response = self.client.models.generate_content(
                            model=self.gemini_model_name,
                            contents=[prompt_for_media, image],
                            config=self.model_config,
                        )
                        media_description = media_desc_response.text.strip()
                        
                        if msg_type == 'audio':
                            entry = f"Usuário enviou um(a) {msg_type}"
                            entry += f": [Conteúdo processado da mídia: {media_description}], mantenha esse conteudo na resposta e envie entre *asteriscos*, abaixo disso um resumo também."
                        elif msg_type == 'image':
                            entry = f"Usuário enviou um(a) {msg_type}"
                            entry += f": [Conteúdo processado da mídia: {media_description}]."
                        elif msg_type == 'voice':
                            entry = media_description
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
            logger.info(f"Texto consolidado para Gemini ({chat_id}): {full_user_input_text[:200]}...")

            if not full_user_input_text:
                logger.info(f"Nenhum texto processável após processar mensagens pendentes para {chat_id}. Limpando e saindo.")
                self._delete_pending_messages(chat_id)
                return # Não há nada para responder

            
            # Gerar resposta do Gemini
            response_text = self.generate_gemini_response(full_user_input_text, chat_id, current_interaction_timestamp)
            logger.info(f"Resposta do Gemini gerada para {chat_id}: {response_text[:100]}...")

            # Enviar resposta ao WhatsApp
            last_message_id_to_reply = all_message_ids[-1] if all_message_ids else None
            if self.send_whatsapp_message(chat_id, response_text, reply_to=last_message_id_to_reply):
                logger.info(f"Resposta enviada com sucesso para {chat_id}.")
            else:
                logger.error(f"Falha ao enviar resposta para {chat_id}.")

            # Atualizar histórico e limpar mensagens pendentes
            self.update_conversation_context(chat_id, full_user_input_text, response_text)
            self._delete_pending_messages(chat_id) # Sucesso, deleta as pendentes
            logger.info(f"Processamento para {chat_id} concluído com sucesso.")

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
            # Construir um prompt para o Gemini com base no histórico e resumo
            # Usaremos o build_context_prompt, mas o "current_prompt_text" será uma instrução
            # para o Gemini gerar a mensagem de reengajamento.
            
            # Obter resumo (se houver) e histórico recente
            summary_ref = self.db.collection("conversation_summaries").document(chat_id)
            summary_doc = summary_ref.get()
            summary_text = summary_doc.get("summary") if summary_doc.exists else ""

            history_list = self._get_conversation_history(chat_id, limit=10) # Últimas 10 trocas
            
            history_parts_reengagement = []
            for msg in history_list:
                role = "Usuário" if not msg.get('is_bot', False) else "Assistente"
                history_parts_reengagement.append(f"{role}: {msg['message_text']}")
            history_str_reengagement = "\n".join(history_parts_reengagement)

            reengagement_instruction = (
                "O usuário deste chat não interage há algum tempo (cerca de 36 horas ou mais).\n"
                "Com base no resumo e/ou no histórico recente da nossa conversa (se disponível abaixo), "
                "gere uma mensagem curta, amigável e personalizada para reengajá-lo. \n"
                "Você pode, por exemplo, perguntar se ele ainda precisa de ajuda com o último tópico discutido, "
                "sugerir continuar a conversa, ou simplesmente perguntar como você pode ser útil hoje. \n"
                "Se não houver histórico ou resumo, apenas envie uma saudação amigável perguntando como pode ajudar.\n"
                "Seja conciso e natural.\n\n"
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

            # Gerar a mensagem de reengajamento usando Gemini (sem tools aqui, só geração de texto)
            reengagement_response = self.client.models.generate_content(
                model=self.gemini_model_name,
                contents=full_reengagement_prompt,
                config=self.model_config
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

    def generate_gemini_response(self, current_input_text: str, chat_id: str, current_message_timestamp: datetime) -> str:
        """Gera resposta do Gemini considerando o contexto completo e usando Google Search tool."""
        try:
            # current_input_text é o texto já processado (incluindo descrições de mídia)
            full_prompt_with_history = self.build_context_prompt(chat_id, current_input_text, current_message_timestamp)
            
            logger.info(f"Prompt final para Gemini (chat {chat_id})")

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
            logger.info(f"Enviando mensagem para WHAPI: to={chat_id}, reply_to={reply_to}, body='{text[:50]}...'")
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
            docs_to_check = list(query.limit(101).stream()) # Um a mais que o limite para saber se passou

            if len(docs_to_check) < 100: # Limite para resumir
                logger.info(f"Chat {chat_id} tem {len(docs_to_check)} mensagens não resumidas. Não é hora de resumir.")
                return
            
            # Pegar as mensagens para resumir (as 100 mais antigas não resumidas)
            query_summarize = (
                self.db.collection("conversation_history")
                .where(filter=FieldFilter("chat_id", "==", chat_id))
                .where(filter=FieldFilter("summarized", "==", False))
                .order_by("timestamp", direction=firestore.Query.ASCENDING) # Mais antigas primeiro
                .limit(100) # Resumir em lotes de 100
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
                "Você é um assistente encarregado de resumir conversas. Abaixo está um trecho de uma conversa entre um Usuário e um Assistente. "
                "Seu objetivo é criar um resumo conciso que capture os pontos principais, decisões tomadas, informações importantes compartilhadas (nomes, locais, datas, preferências, problemas, soluções), "
                "e o sentimento geral ou intenção da conversa. O resumo será usado para dar contexto a futuras interações.\n\n"
                "CONVERSA:\n"
                f"{full_text_for_summary}\n\n"
                "RESUMO CONCISO DA CONVERSA:"
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