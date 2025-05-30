import os
import requests
import time
import re
import logging
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from datetime import datetime, timedelta, timezone
import pytz # Movido para o topo
import random

from bot.config import *
from bot.logger_config import setup_logging
from bot.utils import normalizar_texto
from bot.firestore_manager import FirestoreManager
from bot.reminder_manager import ReminderManager
from bot.gemini_client import GeminiClient
from bot.whatsapp_client import WhatsAppClient

os.environ['TZ'] = 'America/Sao_Paulo'
time.tzset() if hasattr(time, 'tzset') else None

load_dotenv()
logger = setup_logging()
    
class WhatsAppGeminiBot:
    def __init__(self):
        self.reload_env()
        self.firestore_manager = FirestoreManager(project_id="voola-ai")

        if not self.whapi_api_key:
            raise ValueError("Chave API Whapi não configurada no .env para WhatsAppClient.")
        self.whatsapp_client = WhatsAppClient(api_key=self.whapi_api_key)

        self.gemini_client = GeminiClient(
            gemini_api_key=self.gemini_api_key,
            model_name=self.gemini_model_name,
            system_context=self.gemini_context,
            firestore_manager=self.firestore_manager,
            target_timezone_name=TARGET_TIMEZONE_NAME
        )

        self.pending_timeout = 30 # Segundos

        self.reminder_manager = ReminderManager(
            firestore_manager=self.firestore_manager,
            whatsapp_client=self.whatsapp_client,
            gemini_client=self.gemini_client,
            target_timezone_name=TARGET_TIMEZONE_NAME,
        )

        self.target_timezone = pytz.timezone(TARGET_TIMEZONE_NAME)
        logger.info(f"=== INICIALIZAÇÃO TIMEZONE ===")
        logger.info(f"Sistema: {datetime.now().astimezone().tzinfo}")
        logger.info(f"Target: {self.target_timezone}")
        logger.info(f"Hora SP: {datetime.now(self.target_timezone)}")
        logger.info(f"Hora UTC: {datetime.now(timezone.utc)}")
        logger.info(f"=============================")
    
    def _save_pending_message_wrapper(self, chat_id: str, message_payload: Dict[str, Any], from_name: str):
        self.firestore_manager.save_pending_message(chat_id, message_payload, from_name)

    def reload_env(self):
        load_dotenv(override=True)
        self.whapi_api_key = os.getenv('WHAPI_API_KEY')
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        self.gemini_model_name = os.getenv('GEMINI_MODEL')
        self.gemini_context = os.getenv('GEMINI_CONTEXT', '').replace('\\n', '\n')
        
    def update_conversation_context(self, chat_id: str, user_message: str, bot_response: str):
        try:
            self.firestore_manager.save_conversation_history(chat_id, user_message, False)
            self.firestore_manager.update_conversation_context_document(chat_id, user_message, bot_response)
        except Exception as e:
            logger.error(f"Erro ao atualizar contexto: {e}")

    def process_whatsapp_message(self, message: Dict[str, Any]) -> None:
        message_id = message.get('id')
        if not message_id:
            logger.warning("Mensagem sem ID recebida, ignorando."); return

        chat_id = message.get('chat_id')
        if self.firestore_manager.message_exists(message_id) and \
           not self.reminder_manager.pending_reminder_sessions.get(chat_id) and \
           not self.reminder_manager.pending_cancellation_sessions.get(chat_id):
            logger.info(f"Mensagem {message_id} já processada e sem sessão pendente, ignorando."); return

        from_name = message.get('from_name', 'Desconhecido')
        msg_type_whapi = message.get('type', 'text')
        caption = message.get('caption')
        mimetype = message.get('mimetype')
        text_body = ""

        if 'text' in message and isinstance(message['text'], dict):
            text_body = message['text'].get('body', '')
        elif 'body' in message and isinstance(message['body'], str):
            text_body = message['body']
        
        if chat_id in self.reminder_manager.pending_reminder_sessions:
            self.firestore_manager.save_processed_message(message_id, chat_id, text_body, from_name, "text")
            self.firestore_manager.save_conversation_history(chat_id, text_body, False)
            self.reminder_manager._handle_pending_reminder_interaction(chat_id, text_body, message_id)
            return

        if chat_id in self.reminder_manager.pending_cancellation_sessions:
            self.firestore_manager.save_processed_message(message_id, chat_id, text_body, from_name, "text")
            self.firestore_manager.save_conversation_history(chat_id, text_body, False)
            self.reminder_manager._handle_pending_cancellation_interaction(chat_id, text_body, message_id)
            return

        if self.reminder_manager._is_cancel_reminder_request(text_body):
            logger.info(f"Requisição de cancelamento de lembrete detectada para '{text_body}'")
            self.firestore_manager.save_processed_message(message_id, chat_id, text_body, from_name, "text")
            self.firestore_manager.save_conversation_history(chat_id, text_body, False)
            self.reminder_manager._initiate_reminder_cancellation(chat_id, text_body, message_id)
            return

        if self.firestore_manager.message_exists(message_id):
             logger.info(f"Mensagem {message_id} já processada (após checagem de lembrete), ignorando para fluxo Gemini."); return

        media_url = None
        if msg_type_whapi == 'image' and 'image' in message: media_url = message['image'].get('link')
        elif msg_type_whapi in ['audio', 'ptt'] and 'audio' in message: media_url = message['audio'].get('link')
        elif msg_type_whapi == 'video' and 'video' in message: media_url = message['video'].get('link')
        elif msg_type_whapi == 'document' and 'document' in message: media_url = message['document'].get('link')
        elif msg_type_whapi == 'voice' and 'voice' in message: media_url = message['voice'].get('link')

        processed_type_internal = 'text'
        content_to_store = text_body or ""

        if media_url:
            if msg_type_whapi == 'image': processed_type_internal, content_to_store = 'image', media_url
            elif msg_type_whapi in ['audio', 'ptt']: processed_type_internal, content_to_store = 'audio', media_url
            elif msg_type_whapi == 'voice': processed_type_internal, content_to_store = 'voice', media_url
            elif msg_type_whapi == 'document': processed_type_internal, content_to_store = 'document', media_url
            elif msg_type_whapi == 'video': processed_type_internal, content_to_store = 'video', media_url
            elif caption: content_to_store = caption

        text_for_processed_log = caption or text_body or f"[{processed_type_internal} recebida]"
        self.firestore_manager.save_processed_message(message_id, chat_id, text_for_processed_log, from_name, msg_type_whapi)

        if processed_type_internal == 'text' and not content_to_store.strip():
            logger.info(f"Mensagem de texto vazia ou mídia não suportada sem caption para {chat_id}, ignorando."); return

        pending_payload = {
            'type': processed_type_internal, 'content': content_to_store,
            'original_caption': caption, 'mimetype': mimetype,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'message_id': message_id, 'link': media_url
        }
        self._save_pending_message_wrapper(chat_id, pending_payload, from_name)
        logger.info(f"Mensagem de {from_name} ({chat_id}) adicionada à fila pendente. Tipo: {processed_type_internal}.")

    def _check_pending_messages(self, chat_id: str):
        try:
            data = self.firestore_manager.get_pending_messages_doc(chat_id)
            if not data or data.get('processing', False): return

            last_update_dt_str = data.get('last_update')
            last_update_dt = None
            if isinstance(last_update_dt_str, str):
                try: last_update_dt = datetime.fromisoformat(last_update_dt_str.replace('Z', '+00:00'))
                except ValueError: logger.error(f"Formato de last_update string inválido: {last_update_dt_str}"); last_update_dt = datetime.now(timezone.utc)
            elif isinstance(last_update_dt_str, datetime): # Já é datetime
                last_update_dt = last_update_dt_str
            else: # Fallback para outros tipos ou None
                logger.error(f"Tipo de last_update inesperado: {type(last_update_dt_str)}. Usando now()."); last_update_dt = datetime.now(timezone.utc)

            if last_update_dt.tzinfo is None: last_update_dt = last_update_dt.replace(tzinfo=timezone.utc)
            
            if not data.get('messages'):
                self.firestore_manager.delete_pending_messages_doc(chat_id); return

            if (datetime.now(timezone.utc) - last_update_dt).total_seconds() >= self.pending_timeout:
                if self.firestore_manager.mark_chat_as_processing(chat_id):
                    self._process_pending_messages(chat_id)
        except Exception as e:
            logger.error(f"Erro ao verificar mensagens pendentes para {chat_id}: {e}", exc_info=True)
            self.firestore_manager.reset_chat_processing_flag(chat_id)

    def _process_pending_messages(self, chat_id: str):
        try:
            data = self.firestore_manager.get_pending_messages_doc(chat_id)
            if not data: logger.warning(f"Doc pendente {chat_id} sumiu antes do processamento."); return

            pending_msg_list = data.get('messages', [])
            user_from_name = data.get('from_name', 'Usuário')
            if not pending_msg_list:
                self.firestore_manager.delete_pending_messages_doc(chat_id); return
            
            try: pending_msg_list.sort(key=lambda x: datetime.fromisoformat(x['timestamp']))
            except Exception as e_sort: logger.error(f"Erro ao ordenar msgs pendentes para {chat_id}: {e_sort}")

            current_interaction_timestamp_str = pending_msg_list[-1]['timestamp']
            current_interaction_timestamp = datetime.fromisoformat(current_interaction_timestamp_str)
            if current_interaction_timestamp.tzinfo is None:
                current_interaction_timestamp = current_interaction_timestamp.replace(tzinfo=timezone.utc)

            processed_texts_for_gemini = []
            all_message_ids = [msg['message_id'] for msg in pending_msg_list]
            last_user_message_id = all_message_ids[-1] if all_message_ids else None


            for msg_data in pending_msg_list:
                msg_type, content = msg_data['type'], msg_data['content']
                original_caption, mimetype = msg_data.get('original_caption'), msg_data.get('mimetype')

                if msg_type == 'text' and content and content.strip():
                    processed_texts_for_gemini.append(content.strip())
                elif msg_type in ['audio', 'image', 'voice', 'video', 'document']:
                    media_url = content
                    # (A lógica de inferir mimetype e processar mídia com GeminiClient.generate_content_with_media foi simplificada aqui)
                    # Idealmente, GeminiClient.generate_content_with_media lidaria com download e tudo mais.
                    # Por ora, vamos assumir que a descrição da mídia é adicionada a processed_texts_for_gemini.
                    try:
                        # Simulação da chamada ao GeminiClient para processar mídia
                        # Em um cenário real, você passaria a URL ou os bytes para o GeminiClient
                        logger.info(f"Simulando processamento de mídia: {media_url} (mimetype: {mimetype})")
                        media_description = f"[Descrição simulada para {msg_type} em {media_url}]"
                        # media_description = self.gemini_client.generate_content_with_media(prompt_for_media, image_part)
                        
                        entry = f"{user_from_name} enviou um(a) {msg_type}: {media_description}"
                        if original_caption: entry += f" (Legenda: {original_caption})"
                        processed_texts_for_gemini.append(entry)
                    except Exception as e_media:
                        logger.error(f"Erro ao processar mídia {media_url} para {chat_id}: {e_media}", exc_info=True)
                        processed_texts_for_gemini.append(f"[Erro ao processar {msg_type}]")
                        if original_caption: processed_texts_for_gemini.append(f"Legenda: {original_caption}")

            full_user_input_text = "\n".join(processed_texts_for_gemini).strip()
            if not full_user_input_text:
                self.firestore_manager.delete_pending_messages_doc(chat_id); return

            response_text = self.gemini_client.generate_gemini_response(
                chat_id, full_user_input_text, current_interaction_timestamp, user_from_name
            )
            
            reminder_flow_activated = self.reminder_manager.process_gemini_response_for_reminders(
                chat_id, response_text, full_user_input_text, last_user_message_id
            )

            if not reminder_flow_activated:
                if self.whatsapp_client.send_message(chat_id, response_text, reply_to=last_user_message_id):
                    logger.info(f"Resposta do Gemini enviada com sucesso para {chat_id}.")
                    self.update_conversation_context(chat_id, full_user_input_text, response_text)
                else:
                    logger.error(f"Falha ao enviar resposta do Gemini para {chat_id}.")
            
            self.firestore_manager.delete_pending_messages_doc(chat_id)
        except Exception as e:
            logger.error(f"ERRO CRÍTICO ao processar mensagens para {chat_id}: {e}", exc_info=True)
            self.firestore_manager.reset_chat_processing_flag(chat_id)
        finally:
            self._summarize_chat_history_if_needed(chat_id)

    def _check_inactive_chats(self):
        try:
            logger.info("Verificando chats inativos para reengajamento...")
            cutoff_reengagement = datetime.now(timezone.utc) - timedelta(seconds=REENGAGEMENT_TIMEOUT)
            inactive_chat_ids = self.firestore_manager.get_inactive_chat_contexts(cutoff_reengagement)
            processed_chats_for_reengagement = set()

            for chat_id in inactive_chat_ids:
                if chat_id in processed_chats_for_reengagement: continue
                reengagement_log = self.firestore_manager.get_reengagement_log(chat_id)
                if reengagement_log:
                    last_sent = reengagement_log.get("last_sent")
                    if isinstance(last_sent, datetime) and (datetime.now(timezone.utc) - last_sent) < timedelta(hours=23):
                        logger.debug(f"Reengajamento recente para {chat_id}, pulando."); continue
                
                logger.info(f"Chat {chat_id} inativo. Tentando reengajamento inteligente.")
                self._send_reengagement_message(chat_id)
                processed_chats_for_reengagement.add(chat_id)
                time.sleep(1)
        except Exception as e:
            logger.error(f"Erro ao verificar chats inativos: {e}", exc_info=True)

    def _send_reengagement_message(self, chat_id: str):
        try:
            summary_text = self.firestore_manager.get_conversation_summary(chat_id) or ""
            history_list = self.firestore_manager.get_conversation_history(chat_id, limit=5)
            history_parts = [f"{('Usuário' if not m.get('is_bot') else 'Assistente')}: {m['message_text']}" for m in history_list]
            history_str = "\n".join(history_parts)
            reengagement_base_prompt = ("O usuário não interage há algum tempo. Gere uma mensagem de reengajamento CURTA e AMIGÁVEL (máx 2-3 frases). "
                                      "Baseie-se no histórico/resumo, se relevante. Senão, puxe assunto ou faça pergunta aberta. Seja natural.")
            context_for_reengagement = ""
            if summary_text: context_for_reengagement += f"Resumo: {summary_text}\n"
            if history_str: context_for_reengagement += f"Histórico: {history_str}\n"
            if not context_for_reengagement: context_for_reengagement = "Sem histórico anterior.\n"
            full_reengagement_prompt = f"{reengagement_base_prompt}\n{context_for_reengagement}Mensagem de reengajamento:"

            reengagement_message_text = self.gemini_client.generate_gemini_response(
                chat_id=chat_id,
                current_input_text=full_reengagement_prompt,
                current_message_timestamp=datetime.now(timezone.utc)
            )
            if not reengagement_message_text or len(reengagement_message_text) < 10:
                 logger.warning(f"Msg de reengajamento gerada para {chat_id} curta/vazia. Usando fallback.")
                 reengagement_message_text = random.choice(FALLBACK_REENGAGEMENT_MESSAGES)

            if self.whatsapp_client.send_message(chat_id, reengagement_message_text, reply_to=None): # Usa whatsapp_client
                self.firestore_manager.save_reengagement_log(chat_id, reengagement_message_text, hash(full_reengagement_prompt))
                self.firestore_manager.save_conversation_history(chat_id, reengagement_message_text, True)
            else:
                logger.error(f"Falha ao enviar mensagem de reengajamento para {chat_id}.")
        except Exception as e:
            logger.error(f"Erro ao gerar/enviar mensagem de reengajamento para {chat_id}: {e}", exc_info=True)

    def _summarize_chat_history_if_needed(self, chat_id: str):
        try:
            docs_to_check = self.firestore_manager.get_docs_to_summarize(chat_id, limit=26)
            if len(docs_to_check) < 25: return
            
            docs_to_summarize = self.firestore_manager.get_docs_to_summarize(chat_id, limit=100)
            if not docs_to_summarize: return

            logger.info(f"Gerando resumo para {len(docs_to_summarize)} mensagens do chat {chat_id}")
            message_texts = [f"{('Usuário' if not d.to_dict().get('is_bot') else 'Assistente')}: {d.to_dict().get('message_text', '')}" for d in docs_to_summarize]
            full_text = "\n".join(message_texts)

            summary_prompt = (
                "Resuma concisamente a conversa abaixo, capturando pontos principais, decisões, informações importantes e o sentimento geral. "
                "Este resumo será usado para dar contexto a futuras interações. Seja objetivo e use o mínimo de palavras.\n\n"
                f"CONVERSA:\n{full_text}\n\nRESUMO CONCISO:"
            )
            
            summary = self.gemini_client.generate_gemini_response(
                chat_id=chat_id,
                current_input_text=summary_prompt,
                current_message_timestamp=datetime.now(timezone.utc)
            )
            if not summary: logger.warning(f"Resumo gerado para {chat_id} está vazio."); return

            previous_summary = self.firestore_manager.get_conversation_summary(chat_id) or ""
            updated_summary = f"{previous_summary}\n\n[Resumido em {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}]:\n{summary}".strip()
            
            last_doc_data = docs_to_summarize[-1].to_dict()
            last_doc_timestamp = last_doc_data.get("timestamp")

            self.firestore_manager.save_conversation_summary(chat_id, updated_summary, last_doc_timestamp)
            doc_references = [doc.reference for doc in docs_to_summarize] # Pega as referências
            self.firestore_manager.mark_docs_as_summarized(doc_references) # Passa referências
            logger.info(f"{len(docs_to_summarize)} mensagens marcadas como resumidas para {chat_id}.")
        except Exception as e:
            logger.error(f"Erro ao gerar/salvar resumo para {chat_id}: {e}", exc_info=True)

    def run(self):
        try:
            logger.info("Iniciando loop principal de verificação do bot...")
            last_reengagement_check = datetime.now(timezone.utc)
            last_reminder_check = datetime.now(timezone.utc) - timedelta(seconds=REMINDER_CHECK_INTERVAL_SECONDS)
            last_pending_reminder_cleanup = datetime.now(timezone.utc)

            while True:
                try:
                    now = datetime.now(timezone.utc)
                    self._check_all_pending_chats_for_processing()
                    if (now - last_reengagement_check) >= timedelta(hours=1):
                        self._check_inactive_chats()
                        last_reengagement_check = now
                    if (now - last_reminder_check) >= timedelta(seconds=REMINDER_CHECK_INTERVAL_SECONDS):
                        if self.reminder_manager: self.reminder_manager._check_and_send_due_reminders()
                        last_reminder_check = now
                    if (now - last_pending_reminder_cleanup) >= timedelta(seconds=REMINDER_SESSION_TIMEOUT_SECONDS):
                        if self.reminder_manager: self.reminder_manager._cleanup_stale_pending_reminder_sessions()
                        last_pending_reminder_cleanup = now
                except Exception as e:
                    logger.error(f"Erro no ciclo principal de verificação do bot: {e}", exc_info=True)
                time.sleep(PENDING_CHECK_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Bot encerrado manualmente.")
        except Exception as e:
            logger.error(f"Erro fatal no loop principal do bot: {e}", exc_info=True)

    def _check_all_pending_chats_for_processing(self):
        try:
            now = datetime.now(timezone.utc)
            cutoff_for_pending = now - timedelta(seconds=self.pending_timeout)
            chats_to_process_ids = self.firestore_manager.get_pending_chats_for_processing(cutoff_for_pending)
            if chats_to_process_ids:
                logger.info(f"Chats pendentes encontrados para processamento: {len(chats_to_process_ids)}. IDs: {chats_to_process_ids}")
                for chat_id in chats_to_process_ids:
                    self._check_pending_messages(chat_id) 
                    time.sleep(0.5)
        except Exception as e:
            logger.error(f"Erro na verificação de todos os chats pendentes: {e}", exc_info=True)

bot = WhatsAppGeminiBot()

if __name__ == "__main__":
    logger.info("Iniciando o bot WhatsAppGeminiBot em uma thread separada...")
    from threading import Thread
    bot_thread = Thread(target=bot.run, name="BotWorkerThread", daemon=True)
    bot_thread.start()
    try:
        while bot_thread.is_alive():
            bot_thread.join(timeout=1.0)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt recebido no script principal. Encerrando o bot...")
    except Exception as e:
        logger.error(f"Erro fatal no script principal ao aguardar o bot: {e}", exc_info=True)
    finally:
        logger.info("Script principal do bot finalizado.")
