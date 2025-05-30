import logging
from typing import Optional, Dict, Any, List
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter, Query
from datetime import datetime, timezone

# Configura um logger específico para este módulo.
# Isto é preferível a usar o root logger diretamente ou passar instâncias de logger.
logger = logging.getLogger(__name__)

class FirestoreManager:
    def __init__(self, project_id: Optional[str] = None, db_instance: Optional[firestore.Client] = None):
        if db_instance:
            self.db = db_instance
        elif project_id:
            self.db = firestore.Client(project=project_id)
        else:
            raise ValueError("Either project_id or db_instance must be provided.")
        logger.info(f"FirestoreManager inicializado para o projeto: {self.db.project}")

    def get_pending_messages_doc(self, chat_id: str) -> Dict[str, Any]:
        """Obtém o documento de mensagens pendentes para um chat."""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        return {}

    @firestore.transactional
    def save_pending_message_in_transaction(self, transaction: firestore.Transaction,
                                             chat_id: str, new_message: Dict[str, Any],
                                             user_from_name: str) -> None:
        """
        Salva uma nova mensagem pendente dentro de uma transação.
        Este método é destinado a ser chamado por um método que gerencia a transação.
        """
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        snapshot = doc_ref.get(transaction=transaction)
        existing_data = snapshot.to_dict() if snapshot.exists else {}

        messages = existing_data.get('messages', [])
        messages.append(new_message)

        transaction.set(doc_ref, {
            'messages': messages,
            'last_update': datetime.now(timezone.utc), # Firestore SERVER_TIMESTAMP pode ser melhor aqui
            'processing': existing_data.get('processing', False),
            'from_name': user_from_name
        }, merge=True)
        logger.info(f"Mensagem pendente salva para {chat_id} na transação.")

    def save_pending_message(self, chat_id: str, message_payload: Dict[str, Any], from_name: str):
        """
        Armazena mensagem temporariamente com timestamp.
        Gerencia sua própria transação para este salvamento específico.
        """
        # Envolve a lógica transacional em um método que o Firestore pode chamar.
        # A maneira como _save_pending_message era chamada antes sugere que a transação é por operação de salvamento.
        transaction = self.db.transaction()
        self.save_pending_message_in_transaction(transaction, chat_id, message_payload, from_name)
        logger.info(f"Mensagem pendente salva para {chat_id} (transação própria).")


    def delete_pending_messages_doc(self, chat_id: str) -> None:
        """Remove o documento de mensagens pendentes processadas."""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        doc_ref.delete()
        logger.info(f"Documento de mensagens pendentes para {chat_id} removido.")

    def message_exists(self, message_id: str) -> bool:
        """Verifica se a mensagem já foi processada (Firestore)."""
        doc_ref = self.db.collection("processed_messages").document(message_id)
        return doc_ref.get().exists()

    def deactivate_reminder_in_db(self, reminder_id: str) -> bool:
        """Marca um lembrete específico como inativo no Firestore."""
        try:
            reminder_ref = self.db.collection("reminders").document(reminder_id)
            reminder_ref.update({
                "is_active": False,
                "cancelled_at": firestore.SERVER_TIMESTAMP
            })
            logger.info(f"Lembrete {reminder_id} desativado.")
            return True
        except Exception as e:
            logger.error(f"Erro ao desativar lembrete {reminder_id}: {e}", exc_info=True)
            return False

    def get_active_reminders(self, chat_id: str, limit: Optional[int] = 50) -> List[Dict[str, Any]]:
        """Busca lembretes ativos para um usuário, ordenados por tempo."""
        try:
            query_base = (
                self.db.collection("reminders")
                .where(filter=FieldFilter("chat_id", "==", chat_id))
                .where(filter=FieldFilter("is_active", "==", True))
                .order_by("reminder_time_utc", direction=Query.ASCENDING)
            )
            if limit is not None:
                query = query_base.limit(limit)
            else:
                query = query_base

            docs = query.stream()
            reminders = []
            for doc in docs:
                data = doc.to_dict()
                data["id"] = doc.id
                if "reminder_time_utc" in data and isinstance(data["reminder_time_utc"], (int, float)):
                    data["reminder_time_utc"] = datetime.fromtimestamp(data["reminder_time_utc"], tz=timezone.utc)
                elif "reminder_time_utc" in data and isinstance(data["reminder_time_utc"], datetime) and data["reminder_time_utc"].tzinfo is None:
                    data["reminder_time_utc"] = data["reminder_time_utc"].replace(tzinfo=timezone.utc)
                reminders.append(data)
            return reminders
        except Exception as e:
            logger.error(f"Erro ao buscar lembretes ativos para {chat_id}: {e}", exc_info=True)
            return []

    def save_processed_message(self, message_id: str, chat_id: str, text: str, from_name: str, msg_type: str = "text") -> None:
        """Armazena a mensagem processada no Firestore."""
        doc_ref = self.db.collection("processed_messages").document(message_id)
        doc_ref.set({
            "chat_id": chat_id,
            "text_content": text,
            "message_type": msg_type,
            "from_name": from_name,
            "processed_at": firestore.SERVER_TIMESTAMP
        })
        logger.info(f"Mensagem processada {message_id} salva.")

    def save_conversation_history(self, chat_id: str, message_text: str, is_bot: bool) -> None:
        """Armazena o histórico da conversa no Firestore."""
        try:
            col_ref = self.db.collection("conversation_history")
            col_ref.add({
                "chat_id": chat_id,
                "message_text": message_text,
                "is_bot": is_bot,
                "timestamp": firestore.SERVER_TIMESTAMP,
                "summarized": False
            })
        except Exception as e:
            logger.error(f"Erro ao salvar histórico para o chat {chat_id}: {e}", exc_info=True)

    def get_conversation_history(self, chat_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Obtém histórico ordenado cronologicamente, excluindo mensagens já resumidas."""
        try:
            query = (
                self.db.collection("conversation_history")
                .where(filter=FieldFilter("chat_id", "==", chat_id))
                .where(filter=FieldFilter("summarized", "==", False))
                .order_by("timestamp", direction=Query.ASCENDING)
                .limit_to_last(limit)
            )
            docs = query.stream() # Alterado de query.get() para query.stream() para consistência, embora get() seja comum também.
            history = []
            for doc in docs:
                data = doc.to_dict()
                doc_timestamp = data.get('timestamp')
                history_timestamp = None
                if isinstance(doc_timestamp, datetime):
                    history_timestamp = doc_timestamp.timestamp()
                elif doc_timestamp is not None:
                    try:
                        history_timestamp = float(doc_timestamp)
                    except (ValueError, TypeError):
                        logger.warning(f"Timestamp inválido no documento {doc.id}: {doc_timestamp}")

                if 'message_text' in data:
                    history.append({
                        'message_text': data['message_text'],
                        'is_bot': data.get('is_bot', False),
                        'timestamp': history_timestamp
                    })
                else:
                    logger.warning(f"Documento ignorado no histórico (campo 'message_text' ausente): {doc.id}")
            return history
        except Exception as e:
            logger.error(f"Erro ao buscar histórico para {chat_id}: {e}", exc_info=True)
            return []

    def update_conversation_context_document(self, chat_id: str, user_message: str, bot_response: str) -> None:
        """Atualiza o documento de contexto da conversa."""
        try:
            context_ref = self.db.collection("conversation_contexts").document(chat_id)
            context_ref.set({
                "last_updated": firestore.SERVER_TIMESTAMP,
                "last_user_message": user_message,
                "last_bot_response": bot_response
            }, merge=True)
        except Exception as e:
            logger.error(f"Erro ao atualizar contexto Firestore para {chat_id}: {e}", exc_info=True)

    def get_conversation_summary(self, chat_id: str) -> Optional[str]:
        """Obtém o resumo da conversa, se existir."""
        summary_ref = self.db.collection("conversation_summaries").document(chat_id)
        summary_doc = summary_ref.get()
        if summary_doc.exists:
            return summary_doc.get("summary")
        return None

    def save_conversation_summary(self, chat_id: str, summary: str, last_chunk_timestamp: Any) -> None:
        """Salva o resumo da conversa."""
        summary_ref = self.db.collection("conversation_summaries").document(chat_id)
        summary_ref.set({
            "summary": summary,
            "last_updated": firestore.SERVER_TIMESTAMP,
            "last_chunk_timestamp": last_chunk_timestamp
        }, merge=True)
        logger.info(f"Resumo salvo para chat {chat_id}.")

    def get_docs_to_summarize(self, chat_id: str, limit: int = 25) -> list:
        """Pega documentos não resumidos para resumir."""
        query_summarize = (
            self.db.collection("conversation_history")
            .where(filter=FieldFilter("chat_id", "==", chat_id))
            .where(filter=FieldFilter("summarized", "==", False))
            .order_by("timestamp", direction=Query.ASCENDING)
            .limit(limit)
        )
        return list(query_summarize.stream())

    def mark_docs_as_summarized(self, docs_to_mark: list) -> None:
        """Marca uma lista de documentos como resumidos usando um batch."""
        if not docs_to_mark:
            return
        batch = self.db.batch()
        for doc_to_mark in docs_to_mark:
            batch.update(doc_to_mark.reference, {"summarized": True})
        batch.commit()
        logger.info(f"{len(docs_to_mark)} mensagens marcadas como resumidas.")

    def get_pending_chats_for_processing(self, cutoff_for_pending: datetime) -> List[str]:
        """Busca IDs de chats com mensagens pendentes que atingiram o timeout e não estão em processamento."""
        query = (
            self.db.collection("pending_messages")
            .where(filter=FieldFilter("processing", "==", False))
            .where(filter=FieldFilter("last_update", "<=", cutoff_for_pending))
        )
        docs = query.stream()
        return [doc.id for doc in docs]

    @firestore.transactional
    def mark_chat_as_processing_in_transaction(self, transaction: firestore.Transaction, chat_id: str) -> bool:
        """Tenta marcar um chat como 'processing' em uma transação."""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        snapshot = doc_ref.get(transaction=transaction)
        if snapshot.exists and not snapshot.get('processing'):
            transaction.update(doc_ref, {'processing': True, 'last_update': firestore.SERVER_TIMESTAMP})
            return True
        return False

    def mark_chat_as_processing(self, chat_id: str) -> bool:
        """Interface pública para marcar um chat como 'processing'."""
        transaction = self.db.transaction()
        return self.mark_chat_as_processing_in_transaction(transaction, chat_id)

    def reset_chat_processing_flag(self, chat_id: str) -> None:
        """Reseta o flag 'processing' para False, geralmente em caso de erro."""
        try:
            doc_ref = self.db.collection("pending_messages").document(chat_id)
            doc_ref.update({'processing': False})
            logger.info(f"Flag 'processing' resetado para {chat_id}.")
        except Exception as e_update:
            logger.error(f"Erro ao tentar resetar 'processing' para {chat_id}: {e_update}", exc_info=True)

    def get_reengagement_log(self, chat_id: str) -> Optional[Dict[str, Any]]:
        """Obtém o log de reengajamento para um chat."""
        reengagement_log_ref = self.db.collection("reengagement_logs").document(chat_id)
        reengagement_log_doc = reengagement_log_ref.get()
        if reengagement_log_doc.exists:
            return reengagement_log_doc.to_dict()
        return None

    def save_reengagement_log(self, chat_id: str, message_sent: str, prompt_hash: int) -> None:
        """Salva o log de reengajamento."""
        reengagement_log_ref = self.db.collection("reengagement_logs").document(chat_id)
        reengagement_log_ref.set({
            "last_sent": firestore.SERVER_TIMESTAMP,
            "message_sent": message_sent,
            "prompt_used_hash": prompt_hash
        }, merge=True)
        logger.info(f"Log de reengajamento salvo para {chat_id}.")

    def get_inactive_chat_contexts(self, cutoff_reengagement: datetime) -> List[str]:
        """Obtém IDs de chats inativos (baseado em conversation_contexts)."""
        contexts_ref = self.db.collection("conversation_contexts")
        query = contexts_ref.where(filter=FieldFilter("last_updated", "<", cutoff_reengagement)).stream()
        return [doc.id for doc in query]

    def save_reminder_to_db(self, reminder_payload: Dict[str, Any]) -> None:
        """Salva um lembrete completo no Firestore."""
        try:
            doc_ref = self.db.collection("reminders").document()
            doc_ref.set(reminder_payload)
            logger.info(f"Lembrete salvo para {reminder_payload.get('chat_id')} @ {reminder_payload.get('reminder_time_utc')}")
        except Exception as e:
            logger.error(f"Erro ao salvar lembrete para {reminder_payload.get('chat_id')}: {e}", exc_info=True)
            # A lógica de enviar mensagem de erro ao usuário deve ficar na classe principal do Bot.
            raise # Re-lança a exceção para ser tratada pela classe Bot

    def get_due_reminders(self, now_utc: datetime) -> List[Dict[str, Any]]:
        """Busca lembretes ativos que estão devidos."""
        reminders_query = (
            self.db.collection("reminders")
            .where(filter=FieldFilter("is_active", "==", True))
            .where(filter=FieldFilter("reminder_time_utc", "<=", now_utc))
        )
        due_reminders_docs = reminders_query.stream()

        reminders_data = []
        for reminder_doc in due_reminders_docs:
            data = reminder_doc.to_dict()
            data['id'] = reminder_doc.id # Adiciona o ID do documento aos dados
            reminders_data.append(data)
        return reminders_data

    def update_reminder_after_sent(self, reminder_id: str, update_data: Dict[str, Any]) -> None:
        """Atualiza um lembrete após ser enviado (ex: desativa ou reagenda)."""
        try:
            self.db.collection("reminders").document(reminder_id).update(update_data)
            logger.info(f"Lembrete {reminder_id} atualizado após envio.")
        except Exception as e:
            logger.error(f"Erro ao atualizar lembrete {reminder_id} após envio: {e}", exc_info=True)

    def log_missing_chat_id_for_reminder(self, reminder_id: str, reminder_data: Dict[str, Any]) -> None:
        """Loga um erro e desativa um lembrete se o chat_id estiver faltando."""
        logger.error(f"Lembrete ID {reminder_id} não possui chat_id. Dados: {reminder_data}")
        try:
            self.db.collection("reminders").document(reminder_id).update({"is_active": False, "error_log": "Missing chat_id"})
        except Exception as e:
            logger.error(f"Erro ao tentar desativar lembrete {reminder_id} com chat_id faltante: {e}", exc_info=True)

    def log_missing_content_for_reminder(self, reminder_id: str, chat_id: str, reminder_data: Dict[str, Any]) -> None:
        """Loga um erro e desativa um lembrete se o conteúdo estiver faltando."""
        logger.error(f"Lembrete ID {reminder_id} para chat {chat_id} não possui conteúdo. Dados: {reminder_data}")
        try:
            self.db.collection("reminders").document(reminder_id).update({"is_active": False, "error_log": "Missing content"})
        except Exception as e:
            logger.error(f"Erro ao tentar desativar lembrete {reminder_id} com conteúdo faltante: {e}", exc_info=True)

    # Adicione outros métodos conforme necessário, por exemplo, para obter configurações do Firestore, etc.

```
