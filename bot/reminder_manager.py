import logging
import re
import random
from datetime import datetime, timedelta, timezone
from dateutil import parser as dateutil_parser
from dateutil.relativedelta import relativedelta
import pytz
from typing import Optional, Dict, Any, List, Callable

from bot.firestore_manager import FirestoreManager
from bot.utils import normalizar_texto
from bot.config import (
    GEMINI_REMINDER_CONFIRMATION_REGEX,
    REMINDER_STATE_AWAITING_CONTENT,
    REMINDER_STATE_AWAITING_DATETIME,
    REMINDER_STATE_AWAITING_RECURRENCE,
    REMINDER_STATE_AWAITING_CANCELLATION_CHOICE,
    REMINDER_SESSION_TIMEOUT_SECONDS,
    REMINDER_CANCELLATION_SESSION_TIMEOUT_SECONDS,
    REMINDER_CHECK_INTERVAL_SECONDS,
    REMINDER_CONFIRMATION_TEMPLATES,
    REMINDER_CANCEL_KEYWORDS_REGEX,
    PORTUGUESE_DAYS_FOR_PARSING,
    MONTHLY_DAY_SPECIFIC_REGEX,
    RECURRENCE_KEYWORDS,
    REMINDER_REQUEST_KEYWORDS_REGEX,
    leading_words_to_strip_normalized,
    trailing_phrases_to_strip_normalized
)
# Import GeminiClient
from bot.gemini_client import GeminiClient
# Import WhatsAppClient
from bot.whatsapp_client import WhatsAppClient

logger = logging.getLogger(__name__)

class ReminderManager:
    def __init__(self,
                 firestore_manager: FirestoreManager,
                 whatsapp_client: WhatsAppClient, # Alterado: recebe WhatsAppClient
                 gemini_client: GeminiClient,
                 target_timezone_name: str,
                 ):
        self.firestore_manager = firestore_manager
        self.whatsapp_client = whatsapp_client # Armazena a instância do WhatsAppClient
        self.gemini_client = gemini_client
        self.target_timezone = pytz.timezone(target_timezone_name)
        self.logger = logging.getLogger(__name__)

        self.pending_reminder_sessions: Dict[str, Dict[str, Any]] = {}
        self.pending_cancellation_sessions: Dict[str, Dict[str, Any]] = {}

    # _detect_reminder_in_gemini_response e _extract_reminder_from_gemini_response
    # foram movidos para GeminiClient como detect_and_extract_reminder_from_text

    def _is_cancel_reminder_request(self, text: str) -> bool:
        if not text: return False
        return bool(re.search(REMINDER_CANCEL_KEYWORDS_REGEX, normalizar_texto(text), re.IGNORECASE))

    def _is_reminder_request(self, text: str) -> bool: # Pode ser opcional se a detecção for só pelo Gemini
        if not text: return False
        return bool(re.search(REMINDER_REQUEST_KEYWORDS_REGEX, normalizar_texto(text), re.IGNORECASE))

    def _clean_text_for_parsing(self, text: str) -> str:
        processed_text = text.lower()
        monthly_match = re.search(MONTHLY_DAY_SPECIFIC_REGEX, processed_text)
        if monthly_match:
            day_num = monthly_match.group(1) or monthly_match.group(2)
            if day_num and 1 <= int(day_num) <= 31:
                now_local = datetime.now(self.target_timezone)
                target_day = int(day_num)
                try:
                    next_date = now_local.replace(day=target_day)
                    if target_day < now_local.day : # se o dia ja passou, proximo mes
                         next_date = (now_local.replace(day=1) + relativedelta(months=1)).replace(day=target_day)
                except ValueError: # Dia inválido para o mês atual
                    next_date = (now_local.replace(day=1) + relativedelta(months=1)).replace(day=target_day)

                date_str = next_date.strftime('%Y-%m-%d')
                processed_text = re.sub(monthly_match.group(0), date_str, processed_text)
        for pt_day, en_day in PORTUGUESE_DAYS_FOR_PARSING.items():
            processed_text = re.sub(r'\b' + pt_day + r'\b', en_day, processed_text)
        now_in_target_tz = datetime.now(self.target_timezone)
        today_date = now_in_target_tz.strftime('%Y-%m-%d')
        tomorrow_date = (now_in_target_tz + timedelta(days=1)).strftime('%Y-%m-%d')
        after_tomorrow_date = (now_in_target_tz + timedelta(days=2)).strftime('%Y-%m-%d')
        processed_text = re.sub(r'\bhoje\b', f"{today_date} {self.target_timezone.zone}", processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'\bamanhã\b', f"{tomorrow_date} {self.target_timezone.zone}", processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'\bdepois de amanhã\b', f"{after_tomorrow_date} {self.target_timezone.zone}", processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'(\d{1,2})\s*e\s*(\d{1,2})', r'\1:\2', processed_text)
        processed_text = re.sub(r'\b(?:as|às)\s+(\d{1,2})(?!\d|:)\b', r'\1:00', processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'(\d{1,2}:\d{2})(?!:\d{2})', r'\1:00', processed_text)
        processed_text = re.sub(r'próxima\s+', 'next ', processed_text, flags=re.IGNORECASE)
        processed_text = re.sub(r'próximo\s+', 'next ', processed_text, flags=re.IGNORECASE)
        return processed_text

    def _extract_reminder_details_from_text(self, text: str, chat_id: str) -> Dict[str, Any]: # chat_id para logging
        details = {"content": None, "datetime_obj": None, "recurrence": "none", "day_of_month": None, "time_explicitly_provided": False}
        self.logger.info(f"Extraindo detalhes do lembrete (texto user) para {chat_id}: '{text}'")
        payload_text = re.sub(REMINDER_REQUEST_KEYWORDS_REGEX, "", text, flags=re.IGNORECASE).strip()
        for word in leading_words_to_strip_normalized:
            payload_text = re.sub(r"^\s*" + re.escape(word) + r"\s+", "", normalizar_texto(payload_text), flags=re.IGNORECASE).strip()
        if not payload_text: return details

        text_to_parse = payload_text
        monthly_match = re.search(MONTHLY_DAY_SPECIFIC_REGEX, text_to_parse)
        if monthly_match:
            day_num = monthly_match.group(1) or monthly_match.group(2)
            if day_num and 1 <= int(day_num) <= 31:
                details["recurrence"] = "monthly"; details["day_of_month"] = int(day_num)
                text_to_parse = re.sub(monthly_match.group(0), "", text_to_parse).strip()
        else:
            found_recurrence_phrase = ""
            for phrase, key in RECURRENCE_KEYWORDS.items():
                match = re.search(r'\b' + re.escape(normalizar_texto(phrase)) + r'\b', normalizar_texto(text_to_parse), re.IGNORECASE)
                if match:
                    original_phrase_match = re.search(r'\b' + re.escape(phrase) + r'\b', text_to_parse, re.IGNORECASE)
                    if original_phrase_match and len(original_phrase_match.group(0)) > len(found_recurrence_phrase):
                        found_recurrence_phrase = original_phrase_match.group(0); details["recurrence"] = key
            if found_recurrence_phrase: text_to_parse = text_to_parse.replace(found_recurrence_phrase, "").strip()

        cleaned_for_datetime = self._clean_text_for_parsing(text_to_parse)
        initial_content = text_to_parse
        try:
            now_local = datetime.now(self.target_timezone)
            parsed_dt_naive, non_datetime_tokens = dateutil_parser.parse(
                cleaned_for_datetime, fuzzy_with_tokens=True, dayfirst=True,
                default=now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            )
            only_time_provided = all(t.strip().lower() not in cleaned_for_datetime.lower() for t in ['today', 'tomorrow', 'next', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']) and not any(re.search(r'\d{1,2}[-/]\d{1,2}', t) for t in non_datetime_tokens)
            parsed_dt = self.target_timezone.localize(parsed_dt_naive, is_dst=None) if parsed_dt_naive.tzinfo is None else parsed_dt_naive.astimezone(self.target_timezone)
            if only_time_provided:
                details["time_explicitly_provided"] = True
                if parsed_dt.time() < now_local.time() and parsed_dt.date() == now_local.date() : # Adicionado parsed_dt.date() == now_local.date()
                    parsed_dt += timedelta(days=1)
            if details["recurrence"] == "monthly" and details["day_of_month"]:
                target_day = details["day_of_month"]; target_time = parsed_dt.time()
                try: target_date = now_local.replace(day=target_day)
                except ValueError: target_date = (now_local.replace(day=1) + relativedelta(months=1)).replace(day=target_day)
                target_datetime = target_date.replace(hour=target_time.hour, minute=target_time.minute, second=0, microsecond=0)
                if target_datetime <= now_local: target_datetime += relativedelta(months=1)
                parsed_dt = target_datetime
            details["datetime_obj"] = parsed_dt.astimezone(timezone.utc)
            initial_content = " ".join([token.strip() for token in non_datetime_tokens if token.strip()]).strip()
        except (ValueError, TypeError) as e: self.logger.info(f"DateTime parsing failed (text user) for {chat_id}: {e}")

        if initial_content:
            content_words = initial_content.split()
            while content_words and any(normalizar_texto(content_words[-1]) == word for word in trailing_phrases_to_strip_normalized): content_words.pop()
            cleaned_content = " ".join(content_words).strip()
            cleaned_content = re.sub(REMINDER_REQUEST_KEYWORDS_REGEX, "", cleaned_content, flags=re.IGNORECASE).strip()
            if cleaned_content and not any(normalizar_texto(cleaned_content) == word for word in trailing_phrases_to_strip_normalized + leading_words_to_strip_normalized):
                details["content"] = cleaned_content
        return details

    def _initiate_reminder_creation(self, chat_id: str, text: str, message_id: str):
        self.logger.info(f"Iniciando criação de lembrete para {chat_id} com texto: {text}")
        if chat_id in self.pending_reminder_sessions: del self.pending_reminder_sessions[chat_id]

        extracted_details = self._extract_reminder_details_from_text(text, chat_id)
        content, dt_obj_utc, recurrence = extracted_details.get("content"), extracted_details.get("datetime_obj"), extracted_details.get("recurrence", "none")

        session_data = {"content": content, "datetime_obj": dt_obj_utc, "recurrence": recurrence,
                        "original_message_id": message_id, "last_interaction": datetime.now(timezone.utc), "state": ""}

        if not content: session_data["state"] = REMINDER_STATE_AWAITING_CONTENT
        elif not dt_obj_utc: session_data["state"] = REMINDER_STATE_AWAITING_DATETIME

        if session_data["state"]:
            self.pending_reminder_sessions[chat_id] = session_data
            self._ask_for_missing_reminder_info(chat_id, session_data)
        else:
            # Refina o conteúdo ANTES de salvar
            refined_content = self.gemini_client.refine_reminder_content(content, chat_id) if content else None
            if not refined_content : refined_content = content # Fallback

            self._save_reminder_final(chat_id, refined_content, dt_obj_utc, recurrence, message_id, extracted_details.get("day_of_month")) # Passa day_of_month


    def _save_reminder_final(self, chat_id: str, content: str, dt_obj_utc: datetime, recurrence: str, original_message_id: str, day_of_month: Optional[int] = None):
        # Prepara o payload para o FirestoreManager
        reminder_payload = {
            "chat_id": chat_id, "content": content, "reminder_time_utc": dt_obj_utc,
            "recurrence": recurrence, "is_active": True, "created_at": firestore.SERVER_TIMESTAMP, # FirestoreManager pode lidar com isso
            "last_sent_at": None, "original_message_id": original_message_id,
            "original_hour_utc": dt_obj_utc.hour, "original_minute_utc": dt_obj_utc.minute,
            "timezone": self.target_timezone.zone # Salva o nome do timezone
        }
        if recurrence == "monthly" and day_of_month:
            reminder_payload["original_day_of_month"] = day_of_month

        try:
            self.firestore_manager.save_reminder_to_db(reminder_payload)
            dt_local = dt_obj_utc.astimezone(self.target_timezone)
            dt_local_str = dt_local.strftime('%d/%m/%Y às %H:%M')
            confirmation_template = random.choice(REMINDER_CONFIRMATION_TEMPLATES)
            response_text = confirmation_template.format(datetime_str=dt_local_str, content=content)
            if recurrence != "none": response_text += f" (Recorrência: {recurrence})"
            self.whatsapp_client.send_message(chat_id, response_text, reply_to=original_message_id)
            self.firestore_manager.save_conversation_history(chat_id, response_text, True) # Salva a confirmação do bot
        except Exception as e:
            self.logger.error(f"Erro ao salvar lembrete (final) para {chat_id}: {e}", exc_info=True)
            self.whatsapp_client.send_message(chat_id, "Desculpe, não consegui salvar seu lembrete. Tente novamente.", reply_to=original_message_id)
            self.firestore_manager.save_conversation_history(chat_id, "Desculpe, não consegui salvar seu lembrete.", True)


    def _handle_pending_reminder_interaction(self, chat_id: str, text: str, message_id: str):
        if chat_id not in self.pending_reminder_sessions: return
        session = self.pending_reminder_sessions[chat_id]
        session["last_interaction"] = datetime.now(timezone.utc)

        if normalizar_texto(text) in ["cancelar", "cancela"]:
            del self.pending_reminder_sessions[chat_id]
            self.whatsapp_client.send_message(chat_id, "Criação de lembrete cancelada.", reply_to=message_id)
            self.firestore_manager.save_conversation_history(chat_id, "Criação de lembrete cancelada.", True)
            return

        current_state = session["state"]
        if current_state == REMINDER_STATE_AWAITING_CONTENT:
            if text.strip(): session["content"] = text.strip(); session["state"] = ""
            else:
                self.whatsapp_client.send_message(chat_id, "O conteúdo do lembrete não pode ser vazio.", reply_to=message_id)
                self.firestore_manager.save_conversation_history(chat_id, "O conteúdo do lembrete não pode ser vazio.", True)
                return
        elif current_state == REMINDER_STATE_AWAITING_DATETIME:
            try:
                now_local = datetime.now(self.target_timezone)
                cleaned_text = self._clean_text_for_parsing(text)
                parsed_dt_naive = dateutil_parser.parse(cleaned_text, fuzzy=True, dayfirst=True, default=now_local.replace(hour=0, minute=0, second=0, microsecond=0))
                only_time_provided = all(t.strip().lower() not in cleaned_text.lower() for t in ['hoje', 'amanha', 'amanhã', 'proximo', 'próximo', 'segunda', 'terça', 'quarta', 'quinta', 'sexta', 'sabado', 'sábado', 'domingo']) and not re.search(r'\d{1,2}[-/]\d{1,2}', cleaned_text)
                parsed_dt = self.target_timezone.localize(parsed_dt_naive, is_dst=None) if parsed_dt_naive.tzinfo is None else parsed_dt_naive.astimezone(self.target_timezone)
                if only_time_provided and parsed_dt.time() < now_local.time() and parsed_dt.date() == now_local.date(): parsed_dt += timedelta(days=1)
                session["datetime_obj"] = parsed_dt.astimezone(timezone.utc)
                session["state"] = ""
            except Exception as e:
                self.logger.info(f"Não foi possível parsear data/hora de '{text}' para {chat_id}: {e}")
                self.whatsapp_client.send_message(chat_id, "Não entendi a data/hora. Tente: hoje 14:30, amanhã 09:00, 25/12 18:00.", reply_to=message_id)
                self.firestore_manager.save_conversation_history(chat_id, "Não entendi a data/hora.", True)
                return

        if not session.get("content"): session["state"] = REMINDER_STATE_AWAITING_CONTENT
        elif not session.get("datetime_obj"): session["state"] = REMINDER_STATE_AWAITING_DATETIME

        if session["state"]: self._ask_for_missing_reminder_info(chat_id, session)
        else:
            content_to_refine = session["content"]
            # Usa gemini_client para refinar
            refined_content = self.gemini_client.refine_reminder_content(content_to_refine, chat_id) if content_to_refine else None
            if not refined_content: refined_content = content_to_refine # Fallback

            self._save_reminder_final(chat_id, refined_content, session["datetime_obj"], session.get("recurrence", "none"), session["original_message_id"], session.get("day_of_month"))
            if chat_id in self.pending_reminder_sessions: del self.pending_reminder_sessions[chat_id]

    def _ask_for_missing_reminder_info(self, chat_id: str, session_data: Dict[str, Any]):
        question = ""
        if session_data["state"] == REMINDER_STATE_AWAITING_CONTENT: question = "Ok! Qual é o conteúdo do lembrete?"
        elif session_data["state"] == REMINDER_STATE_AWAITING_DATETIME: question = "Entendido. Para quando devo agendar?"
        # elif session_data["state"] == REMINDER_STATE_AWAITING_RECURRENCE: question = "Repetir? (diariamente, semanalmente, etc.)"
        if question:
            self.whatsapp_client.send_message(chat_id, question, reply_to=session_data["original_message_id"])
            self.firestore_manager.save_conversation_history(chat_id, question, True) # Salva pergunta do bot

    def _initiate_reminder_cancellation(self, chat_id: str, text: str, message_id: str):
        self.logger.info(f"Iniciando cancelamento de lembrete para {chat_id} com texto: '{text}'")
        if chat_id in self.pending_cancellation_sessions: del self.pending_cancellation_sessions[chat_id]
        normalized_text = normalizar_texto(text)

        if re.search(r'\btodos\b', normalized_text, re.IGNORECASE):
            all_active_reminders = self.firestore_manager.get_active_reminders(chat_id, limit=None)
            if not all_active_reminders:
                self.whatsapp_client.send_message(chat_id, "Você não tem lembretes ativos.", reply_to=message_id)
                self.firestore_manager.save_conversation_history(chat_id, "Você não tem lembretes ativos.", True)
                return
            cancelled_count = sum(1 for r in all_active_reminders if self.firestore_manager.deactivate_reminder_in_db(r["id"]))
            response_text = f"{cancelled_count} lembrete(s) cancelados." if cancelled_count > 0 else "Não foi possível cancelar os lembretes."
            self.whatsapp_client.send_message(chat_id, response_text, reply_to=message_id)
            self.firestore_manager.save_conversation_history(chat_id, response_text, True)
            return

        active_reminders = self.firestore_manager.get_active_reminders(chat_id, limit=10)
        if not active_reminders:
            self.whatsapp_client.send_message(chat_id, "Você não tem lembretes ativos.", reply_to=message_id)
            self.firestore_manager.save_conversation_history(chat_id, "Você não tem lembretes ativos.", True)
            return

        options, response_parts = [], ["Qual lembrete cancelar?"]
        for i, r in enumerate(active_reminders):
            dt_local = r["reminder_time_utc"].astimezone(self.target_timezone).strftime('%d/%m %H:%M')
            summary = f"'{r['content'][:30]}' ({dt_local})"
            response_parts.append(f"{i+1}. {summary}")
            options.append({"id": r["id"], "text_summary": summary})

        if len(active_reminders) == 1: response_parts.append("\nDigite '1' ou 'sim' para cancelar, ou 'não'.")
        else: response_parts.append("\nDigite o número, 'todos' (listados) ou 'nenhum'.")

        self.pending_cancellation_sessions[chat_id] = {
            "state": REMINDER_STATE_AWAITING_CANCELLATION_CHOICE, "reminders_options": options,
            "original_message_id": message_id, "last_interaction": datetime.now(timezone.utc)
        }
        self.whatsapp_client.send_message(chat_id, "\n".join(response_parts), reply_to=message_id)
        self.firestore_manager.save_conversation_history(chat_id, "\n".join(response_parts), True)


    def _handle_pending_cancellation_interaction(self, chat_id: str, text: str, message_id: str):
        if chat_id not in self.pending_cancellation_sessions: return
        session = self.pending_cancellation_sessions[chat_id]
        session["last_interaction"] = datetime.now(timezone.utc)
        user_input = normalizar_texto(text.strip())
        orig_msg_id = session.get("original_message_id", message_id)
        options = session.get("reminders_options", [])

        if user_input in ["cancelar", "nenhum", "nao"]:
            del self.pending_cancellation_sessions[chat_id]
            self.whatsapp_client.send_message(chat_id, "Ok, nenhum lembrete cancelado.", reply_to=orig_msg_id)
            self.firestore_manager.save_conversation_history(chat_id, "Ok, nenhum lembrete cancelado.", True)
            return

        response_text = ""
        if user_input == "todos":
            cancelled_count = sum(1 for opt in options if self.firestore_manager.deactivate_reminder_in_db(opt["id"]))
            response_text = f"{cancelled_count} lembretes da lista cancelados." if cancelled_count > 0 else "Não foi possível cancelar."
        elif len(options) == 1 and user_input in ["sim", "1", "s"]:
            if self.firestore_manager.deactivate_reminder_in_db(options[0]["id"]): response_text = f"Lembrete '{options[0]['text_summary']}' cancelado."
            else: response_text = "Não foi possível cancelar."
        else:
            try:
                idx = int(user_input) - 1
                if 0 <= idx < len(options):
                    if self.firestore_manager.deactivate_reminder_in_db(options[idx]["id"]): response_text = f"Lembrete '{options[idx]['text_summary']}' cancelado."
                    else: response_text = "Não foi possível cancelar."
                else: response_text = "Opção inválida."
            except ValueError: response_text = "Escolha inválida. Digite o número, 'todos' ou 'nenhum'."

        if "cancelado" in response_text or "Não foi possível" in response_text or "Opção inválida" in response_text or "Escolha inválida" in response_text:
            if "cancelado" in response_text or ("Não foi possível" in response_text and "lista" not in response_text) : # Ação finalizada ou falha individual
                 if chat_id in self.pending_cancellation_sessions: del self.pending_cancellation_sessions[chat_id]
            self.whatsapp_client.send_message(chat_id, response_text, reply_to=orig_msg_id if ("cancelado" in response_text or "Não foi possível" in response_text) else message_id)
        # Removido o else para enviar "Por favor, tente novamente" pois a mensagem de erro já é suficiente.
        self.firestore_manager.save_conversation_history(chat_id, response_text, True)


    def _get_next_occurrence(self, last_occurrence_utc: datetime, recurrence: str, original_hour_utc: int, original_minute_utc: int) -> Optional[datetime]:
        base_time = last_occurrence_utc.replace(hour=original_hour_utc, minute=original_minute_utc, second=0, microsecond=0)
        next_occurrence = None
        if recurrence == "daily": next_occurrence = base_time + timedelta(days=1)
        elif recurrence == "weekly": next_occurrence = base_time + timedelta(weeks=1)
        elif recurrence == "monthly": next_occurrence = base_time + relativedelta(months=1)
        elif recurrence == "yearly": next_occurrence = base_time + relativedelta(years=1)

        if next_occurrence and next_occurrence <= last_occurrence_utc: # Ensure it's in the future
             now_utc = datetime.now(timezone.utc)
             while next_occurrence <= now_utc:
                if recurrence == "daily": next_occurrence += timedelta(days=1); continue
                if recurrence == "weekly": next_occurrence += timedelta(weeks=1); continue
                if recurrence == "monthly": next_occurrence += relativedelta(months=1); continue
                if recurrence == "yearly": next_occurrence += relativedelta(years=1); continue
                break
        return next_occurrence

    def _check_and_send_due_reminders(self):
        now_utc = datetime.now(timezone.utc)
        try:
            due_reminders_data = self.firestore_manager.get_due_reminders(now_utc)
            for r_data in due_reminders_data:
                chat_id, content, r_id = r_data.get("chat_id"), r_data.get("content"), r_data.get("id")
                if not chat_id: self.firestore_manager.log_missing_chat_id_for_reminder(r_id, r_data); continue
                if not content: self.firestore_manager.log_missing_content_for_reminder(r_id, chat_id, r_data); continue

                r_time_utc = r_data["reminder_time_utc"]
                if not isinstance(r_time_utc, datetime): # Garantir que é datetime
                    r_time_utc = datetime.fromtimestamp(r_time_utc.timestamp(), tz=timezone.utc) #Tenta converter se for Timestamp do Firestore
                if r_time_utc.tzinfo is None: r_time_utc = r_time_utc.replace(tzinfo=timezone.utc)

                self.logger.info(f"Enviando lembrete ID {r_id} para {chat_id}")
                msg = f"⏰ Lembrete: {content}" # Simplificado por enquanto
                success = self.whatsapp_client.send_message(chat_id, msg, None)

                if success:
                    self.firestore_manager.save_conversation_history(chat_id, msg, True)
                    update_payload = {"last_sent_at": firestore.SERVER_TIMESTAMP} # FirestoreManager pode lidar com isso
                    recurrence = r_data.get("recurrence", "none")
                    if recurrence == "none": update_payload["is_active"] = False
                    else:
                        orig_hour = r_data.get("original_hour_utc", r_time_utc.hour)
                        orig_min = r_data.get("original_minute_utc", r_time_utc.minute)
                        next_occ = self._get_next_occurrence(r_time_utc, recurrence, orig_hour, orig_min)
                        if next_occ: update_payload["reminder_time_utc"] = next_occ
                        else: update_payload["is_active"] = False; self.logger.warning(f"Não foi possível reagendar {r_id}")
                    self.firestore_manager.update_reminder_after_sent(r_id, update_payload)
                else: self.logger.error(f"Falha ao enviar lembrete ID {r_id} para {chat_id}.")
        except Exception as e: self.logger.error(f"Erro ao verificar/enviar lembretes: {e}", exc_info=True)

    def _cleanup_stale_pending_reminder_sessions(self):
        now = datetime.now(timezone.utc)
        stale_rem_sessions = [k for k, v in self.pending_reminder_sessions.items() if (now - v.get("last_interaction", now)).total_seconds() > REMINDER_SESSION_TIMEOUT_SECONDS]
        for k in stale_rem_sessions: del self.pending_reminder_sessions[k]; self.logger.info(f"Sessão de lembrete para {k} expirou.")

        stale_cancel_sessions = [k for k, v in self.pending_cancellation_sessions.items() if (now - v.get("last_interaction", now)).total_seconds() > REMINDER_CANCELLATION_SESSION_TIMEOUT_SECONDS]
        for k in stale_cancel_sessions: del self.pending_cancellation_sessions[k]; self.logger.info(f"Sessão de cancelamento para {k} expirou.")

    def process_gemini_response_for_reminders(self, chat_id: str, gemini_response_text: str, original_user_input: str, message_id: str) -> bool:
        """
        Processa a resposta do Gemini para detectar e criar lembretes.
        Retorna True se um lembrete foi detectado (e uma sessão iniciada ou lembrete salvo), False caso contrário.
        """
        reminder_details = self.gemini_client.detect_and_extract_reminder_from_text(gemini_response_text)

        if reminder_details.get("found"):
            self.logger.info(f"Lembrete detectado na resposta do Gemini para {chat_id}")

            # Complementar com detalhes do input original do usuário se necessário
            if not reminder_details.get("content") or not reminder_details.get("datetime_obj"):
                user_text_details = self._extract_reminder_details_from_text(original_user_input, chat_id)
                if not reminder_details.get("content") and user_text_details.get("content"):
                    reminder_details["content"] = user_text_details["content"]
                if not reminder_details.get("datetime_obj") and user_text_details.get("datetime_obj"):
                    reminder_details["datetime_obj"] = user_text_details["datetime_obj"]
                    # Se pegamos a data/hora do usuário, a recorrência também deveria vir de lá, ou ser reavaliada.
                    if user_text_details.get("recurrence") != "none" and reminder_details.get("recurrence", "none") == "none":
                         reminder_details["recurrence"] = user_text_details.get("recurrence")
                    if user_text_details.get("day_of_month") and not reminder_details.get("day_of_month"):
                        reminder_details["day_of_month"] = user_text_details.get("day_of_month")


            # Iniciar criação do lembrete com os detalhes combinados (da Gemini e/ou do usuário)
            # _initiate_reminder_creation_with_details vai refinar o conteúdo e então salvar ou pedir mais info.
            self._initiate_reminder_creation_with_details(
                chat_id,
                reminder_details.get("content"),
                reminder_details.get("datetime_obj"),
                reminder_details.get("recurrence", "none"),
                message_id, # Usar o ID da mensagem atual do usuário para a sessão
                reminder_details.get("day_of_month")
            )
            return True # Indica que um fluxo de lembrete foi iniciado/concluído.
        return False # Nenhum lembrete detectado / fluxo iniciado.

    def _initiate_reminder_creation_with_details(self, chat_id: str, content: Optional[str],
                                                dt_obj_utc: Optional[datetime], recurrence: str,
                                                message_id: str, day_of_month: Optional[int]):
        """Inicia a criação de lembrete com detalhes já extraídos (ou parcialmente extraídos)."""
        self.logger.info(f"Iniciando criação de lembrete (com detalhes) para {chat_id}. Content: {content}, DateTime: {dt_obj_utc}")
        if chat_id in self.pending_reminder_sessions: del self.pending_reminder_sessions[chat_id]

        session_data = {
            "content": content, "datetime_obj": dt_obj_utc, "recurrence": recurrence,
            "original_message_id": message_id, "last_interaction": datetime.now(timezone.utc),
            "state": "", "day_of_month": day_of_month
        }

        if not content: session_data["state"] = REMINDER_STATE_AWAITING_CONTENT
        elif not dt_obj_utc: session_data["state"] = REMINDER_STATE_AWAITING_DATETIME

        if session_data["state"]:
            self.pending_reminder_sessions[chat_id] = session_data
            self._ask_for_missing_reminder_info(chat_id, session_data)
        else:
            refined_content = self.gemini_client.refine_reminder_content(content, chat_id) if content else None
            if not refined_content: refined_content = content # Fallback
            self._save_reminder_final(chat_id, refined_content, dt_obj_utc, recurrence, message_id, day_of_month)
