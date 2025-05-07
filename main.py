import os
import requests
import google.generativeai as genai
from google.genai.types import Tool, GenerateContentConfig, GoogleSearch 
import time
import re
import logging
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from datetime import datetime, timedelta, timezone

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
    PENDING_CHECK_INTERVAL = 10
    REENGAGEMENT_TIMEOUT = 43200  # 12 horas em segundos
    REENGAGEMENT_MESSAGES = [
        "Oi!Está tudo bem por aí? Posso ajudar com algo?",
        "Estava pensando em você! Como posso ajudar?",
        "Oi! Espero que esteja tudo certo. Estou aqui se precisar de algo!",
        "Oi, tudo bem? Se quiser conversar comigo, estou à disposição!",
        "Oi! Como posso ajudar você hoje?",
        "Oi! Estou aqui para ajudar. Como posso ser útil?",
        "Oi! Se precisar de algo, estou por aqui.",
        "Oi! Estou aqui para ajudar. O que você precisa?",
    ]
    def __init__(self):
        self.reload_env()
        self.db = firestore.Client(project="voola-ai")
        self.pending_timeout = 30  # Timeout para mensagens pendentes (em segundos)
        
        if not all([self.whapi_api_key, self.gemini_api_key]):
            raise ValueError("Chaves API não configuradas no .env")
        
        self.setup_apis()

    def _get_pending_messages(self, chat_id: str) -> Dict[str, Any]:
        """Obtém mensagens pendentes para um chat"""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        return doc_ref.get().to_dict() or {}
    
    def _save_pending_message(self, chat_id: str, message: Dict[str, Any]):
        """Armazena mensagem temporariamente com timestamp"""
        doc_ref = self.db.collection("pending_messages").document(chat_id)

        existing = self._get_pending_messages(chat_id)
        messages = existing.get('messages', [])
        messages.append(message)

        doc_ref.set({
            'messages': messages,
            'last_update': datetime.now(timezone.utc),
            'processing': False  # evitar processamento duplicado
        })

    def _delete_pending_messages(self, chat_id: str):
        """Remove mensagens processadas"""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
        doc_ref.delete()

    def _message_exists(self, message_id: str) -> bool:
        """Verifica se a mensagem já foi processada (Firestore)"""
        doc_ref = self.db.collection("processed_messages").document(message_id)
        return doc_ref.get().exists

    def _save_message(self, message_id: str, chat_id: str, text: str, from_name: str):
        """Armazena a mensagem no Firestore"""
        doc_ref = self.db.collection("processed_messages").document(message_id)
        doc_ref.set({
            "chat_id": chat_id,
            "text_content": text,
            "from_name": from_name,
            "processed_at": firestore.SERVER_TIMESTAMP
        })

    def _save_conversation_history(self, chat_id: str, message_text: str, is_bot: bool):
        """Armazena o histórico da conversa no Firestore."""
        try:
            # Armazena apenas mensagens do usuário
            if not is_bot:
                col_ref = self.db.collection("conversation_history")
                col_ref.add({
                    "chat_id": chat_id,
                    "message_text": message_text,
                    "timestamp": firestore.SERVER_TIMESTAMP,
                    "summarized": False  # Marca como não resumido
                })
        except Exception as e:
            logger.error(f"Erro ao salvar histórico para o chat {chat_id}: {e}")

    def _get_conversation_history(self, chat_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Obtém histórico ordenado cronologicamente, excluindo mensagens já resumidas."""
        try:
            query = (
                self.db.collection("conversation_history")
                .where("chat_id", "==", chat_id)
                .where("summarized", "==", False)  # Exclui mensagens já resumidas
                .order_by("timestamp", direction=firestore.Query.ASCENDING)
                .limit_to_last(limit)
            )
            docs = query.get()

            history = []
            for doc in docs:
                data = doc.to_dict()
                if 'message_text' in data:  # Verifica se o campo 'message_text' existe
                    history.append({
                        'message_text': data['message_text'],
                        'timestamp': data['timestamp'].timestamp() if data.get('timestamp') else None
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
        self.gemini_model = os.getenv('GEMINI_MODEL')
        self.gemini_context = os.getenv('GEMINI_CONTEXT', '').replace('\\n', '\n')
        
    def setup_apis(self):
        """Configura as conexões com as APIs"""
        try:
            # Configura Gemini
            genai.configure(api_key=self.gemini_api_key)

            # Configura o modelo Gemini
            self.model = genai.GenerativeModel(
                model_name=self.gemini_model,
                system_instruction=self.gemini_context  
            )

            # Configura ferramenta de busca na web
            self.search_tool = Tool(
                google_search=GoogleSearch()
            )

            logger.info("Configuração do Gemini concluída")
            
            # Testa conexão com Whapi
            self.test_whapi_connection()
            
        except Exception as e:
            logger.error(f"Erro na configuração das APIs: {e}")
            raise

    def update_conversation_context(self, chat_id: str, user_message: str, bot_response: str):
        """Atualiza o contexto diretamente no Firestore"""
        try:
            # Salva histórico no Firestore
            self._save_conversation_history(chat_id, user_message, False)
            
            # Atualiza último contexto
            context_ref = self.db.collection("conversation_contexts").document(chat_id)
            context_ref.set({
                "last_updated": firestore.SERVER_TIMESTAMP,
                "last_user_message": user_message,
                "last_bot_response": bot_response
            }, merge=True)
            
        except Exception as e:
            logger.error(f"Erro ao atualizar contexto: {e}")

    def build_context_prompt(self, chat_id: str, current_prompt: str) -> str:
        """Constrói o prompt com histórico formatado corretamente, incluindo o resumo."""
        try:
            # Buscar o resumo do Firestore
            summary_ref = self.db.collection("conversation_summaries").document(chat_id)
            summary_doc = summary_ref.get()
            summary = summary_doc.get("summary") if summary_doc.exists else ""

            # Obter o histórico da conversa (apenas mensagens não resumidas)
            history = self._get_conversation_history(chat_id, limit=500)

            if not history and not summary:
                return current_prompt

            # Ordenar cronologicamente e formatar o histórico se for do bot ignorar
            sorted_history = sorted(history, key=lambda x: x['timestamp'])
            context_str = "\n".join(
                f"Usuário: {msg['message_text']}" for msg in sorted_history if not msg.get('is_bot', False)
            )

            # Construir o prompt final
            return (
                f"### Resumo da conversa ###\n{summary}\n\n"
                f"### Histórico da conversa ###\n{context_str}\n\n"
                "### Nova mensagem ###\n"
                f"Usuário: {current_prompt}"
            )

        except Exception as e:
            logger.error(f"Erro ao construir contexto para o chat {chat_id}: {e}")
            return current_prompt
    
    def test_whapi_connection(self):
        """Testa a conexão com a API Whapi"""
        try:
            response = requests.get(
                "https://gate.whapi.cloud/settings/",
                headers={"Authorization": f"Bearer {self.whapi_api_key}"},
                timeout=10
            )
            response.raise_for_status()
            logger.info("Conexão com Whapi.cloud verificada com sucesso")
            return True
        except Exception as e:
            logger.error(f"Falha na conexão com Whapi.cloud: {e}")
            raise

    def test_gemini_connection(self):
        """Testa a conexão com a API Gemini"""
        try:
            response = self.model.generate_content("Teste de conexão")
            logger.info("Conexão com Gemini verificada com sucesso")
            return True
        except Exception as e:
            logger.error(f"Falha na conexão com Gemini: {e}")
            raise

    def _delete_is_bot_true(self):
        """Deleta todas as mensagens de todas as coleções onde is_bot = True."""
        try:
            # Lista de coleções relevantes
            collections_to_check = ["conversation_history", "pending_messages", "processed_messages"]

            for collection_name in collections_to_check:
                logger.info(f"Verificando coleção: {collection_name}")
                collection_ref = self.db.collection(collection_name)

                # Consulta para encontrar documentos onde is_bot = True
                query = collection_ref.where("is_bot", "==", True)
                docs = query.stream()

                # Deletar documentos encontrados
                count = 0
                for doc in docs:
                    doc.reference.delete()
                    count += 1

                logger.info(f"Total de documentos deletados na coleção {collection_name}: {count}")

        except Exception as e:
            logger.error(f"Erro ao deletar mensagens com is_bot = True: {e}", exc_info=True)

    def process_whatsapp_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Armazena mensagem temporariamente e inicia timer"""
        logger.info(f"Mensagem recebida: {message}")

        message_id = message.get('id')
        if not message_id or self._message_exists(message_id):
            return None

        chat_id = message.get('chat_id')
        from_name = message.get('from_name')
        texto = message.get('text', {}).get('body', '').strip()

        # Salvar no histórico
        self._save_message(message_id, chat_id, texto, from_name)

        # Armazenar em pending_messages
        self._save_pending_message(chat_id, {
            'text': texto,
            'timestamp': datetime.now(timezone.utc),
            'message_id': message_id
        })

        # Iniciar verificação de timeout
        self._check_pending_messages(chat_id)

        return None  # Não processa imediatamente
    
    def _check_pending_messages(self, chat_id: str):
        """Verifica se deve processar as mensagens acumuladas"""
        doc_ref = self.db.collection("pending_messages").document(chat_id)

        try:
            # Verifica sem transação primeiro para evitar locks desnecessários
            doc = doc_ref.get()
            if not doc.exists:
                return

            data = doc.to_dict()
            if data.get('processing', False):
                return

            last_update = data['last_update']
            if isinstance(last_update, datetime):
                last_update = last_update.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)
            timeout = (now - last_update).total_seconds()

            if timeout >= self.pending_timeout:
                # Marca como processando ANTES de processar
                doc_ref.update({'processing': True})
                self._process_pending_messages(chat_id)

        except Exception as e:
            logger.error(f"Erro ao verificar mensagens pendentes: {e}")
            doc_ref.update({'processing': False})

    def _process_pending_messages(self, chat_id: str):
        """Processa todas as mensagens acumuladas"""
        doc_ref = self.db.collection("pending_messages").document(chat_id)

        try:
            logger.info(f"Iniciando processamento para {chat_id}")

            doc = doc_ref.get()
            if not doc.exists:
                logger.warning(f"Chat {chat_id} não encontrado")
                return

            data = doc.to_dict()
            messages = data.get('messages', [])

            if not messages:
                logger.warning(f"Nenhuma mensagem pendente para {chat_id}")
                doc_ref.delete()
                return

            logger.info(f"Processando {len(messages)} mensagens para {chat_id}")

            # Ordenar e concatenar mensagens
            messages = sorted(messages, key=lambda x: x['timestamp'])
            full_text = "\n".join([msg['text'] for msg in messages])
            message_ids = [msg['message_id'] for msg in messages]

            # Gerar resposta
            logger.info(f"Gerando resposta Gemini para {chat_id}")
            response_text = self.generate_gemini_response(full_text, chat_id)
            logger.info(f"Resposta gerada: {response_text[:50]}...")

            # Enviar resposta
            last_message_id = message_ids[-1]
            logger.info(f"Enviando resposta para {chat_id} (reply_to: {last_message_id})")
            if self.send_whatsapp_message(chat_id, response_text, last_message_id):
                logger.info("Mensagem enviada com sucesso")
            else:
                logger.error("Falha ao enviar mensagem")

            # Atualizar histórico
            self.update_conversation_context(chat_id, full_text, response_text)
            doc_ref.delete()
            logger.info(f"Processamento concluído para {chat_id}")

        except Exception as e:
            logger.error(f"ERRO CRÍTICO ao processar {chat_id}: {str(e)}", exc_info=True)
            doc_ref.update({'processing': False})

    def _check_inactive_chats(self):
        """Verifica chats inativos para reengajamento"""
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.REENGAGEMENT_TIMEOUT)

            # Busca apenas chats inativos diretamente no Firestore
            chats_ref = self.db.collection("conversation_history")
            query = (
                chats_ref
                .where("timestamp", "<", cutoff)  # Filtra apenas chats inativos
                .order_by("timestamp", direction=firestore.Query.DESCENDING)
            )

            # Processa os chats inativos
            for doc in query.stream():
                chat_id = doc.get("chat_id")
                last_msg_time = doc.get("timestamp")

                if chat_id and last_msg_time:
                    self._send_reengagement_message(chat_id)

        except Exception as e:
            logger.error(f"Erro ao verificar chats inativos: {e}", exc_info=True)

    def _send_reengagement_message(self, chat_id: str):
        """Envia mensagem de follow-up para chats inativos"""
        try:
            # Verifica se já foi enviada mensagem recentemente
            last_msg_ref = self.db.collection("reengagement_logs").document(chat_id)
            last_msg = last_msg_ref.get()

            if last_msg.exists:
                last_sent = last_msg.get("last_sent")
                if (datetime.now(timezone.utc)) - last_sent < timedelta(hours=12):
                    return

            # Seleciona mensagem aleatória
            import random
            message = random.choice(self.REENGAGEMENT_MESSAGES)

            # Envia mensagem
            if self.send_whatsapp_message(chat_id, message, None):
                # Registra no log
                last_msg_ref.set({
                    "last_sent": datetime.now(timezone.utc),
                    "message": message
                })
                logger.info(f"Mensagem de reengajamento enviada para {chat_id}")

        except Exception as e:
            logger.error(f"Erro ao enviar reengajamento: {e}")

    def generate_gemini_response(self, prompt: str, chat_id: str) -> str:
        """Gera resposta considerando o contexto completo"""
        try:
            full_prompt = self.build_context_prompt(chat_id, prompt)
            logger.info(f"Prompt enviado ao Gemini:\n{full_prompt}")
            response = self.model.generate_content(full_prompt)
            return response.text.strip()
        except Exception as e:
            logger.error(f"Erro no Gemini: {e}")
            return "Desculpe, ocorreu um erro. Por favor, reformule sua pergunta."

    def send_whatsapp_message(self, chat_id: str, text: str, reply_to: str) -> bool:
        """Envia mensagem formatada para o WhatsApp"""
        self._delete_is_bot_true()
        if not text or not chat_id:
            logger.error("Dados inválidos para envio")
            return False

        payload = {
            "to": chat_id,
            "body": text,
            "reply": reply_to
        }

        try:
            logger.info(f"Enviando para WHAPI: {payload}")
            response = requests.post(
                "https://gate.whapi.cloud/messages/text",
                headers={
                    "Authorization": f"Bearer {self.whapi_api_key}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=10
            )

            logger.info(f"Resposta WHAPI: {response.status_code} - {response.text}")
            return response.status_code == 200

        except Exception as e:
            logger.error(f"Falha no envio: {str(e)}")
            return False

    def _summarize_chat_history(self, chat_id: str):
        """Gera um resumo das últimas 100 mensagens e marca como resumidas."""
        try:
            # Obter as últimas 100 mensagens não resumidas
            query = (
                self.db.collection("conversation_history")
                .where("chat_id", "==", chat_id)
                .where("summarized", "==", False)  # Apenas mensagens não resumidas
                .order_by("timestamp", direction=firestore.Query.ASCENDING)
                .limit(100)
            )
            docs = query.get()

            if len(docs) < 100:
                logger.info(f"Chat {chat_id} ainda não possui 100 mensagens não resumidas.")
                return

            # Concatenar mensagens para enviar ao Gemini
            messages = [doc.get('message_text') for doc in docs]
            full_text = "\n".join(messages)

            # Gerar resumo com o Gemini
            logger.info(f"Gerando resumo para o chat {chat_id}")
            summary_prompt = (
                "Resuma as informações importantes das mensagens abaixo, incluindo nomes, "
                "acontecimentos, eventos importantes, sentimentos, detalhes pessoais e o que você julgar importante:\n\n"
                f"{full_text}"
            )
            response = self.model.generate_content(summary_prompt)
            summary = response.text.strip()

            # Armazenar o resumo no Firestore
            summary_ref = self.db.collection("conversation_summaries").document(chat_id)
            summary_ref.set({
                "summary": summary,
                "last_updated": firestore.SERVER_TIMESTAMP
            })ERROR - Erro ao buscar histórico: "'message_text' is not contained in the data"
            logger.info(f"Resumo gerado e armazenado para o chat {chat_id}")

            # Marcar as mensagens como resumidas
            batch = self.db.batch()
            for doc in docs:
                doc_ref = doc.reference
                batch.update(doc_ref, {"summarized": True})
            batch.commit()
            logger.info(f"Mensagens marcadas como resumidas para o chat {chat_id}")

        except Exception as e:
            logger.error(f"Erro ao gerar resumo para o chat {chat_id}: {e}", exc_info=True)

    def run(self):
        """Inicia verificação periódica de mensagens pendentes"""
        try:
            logger.info("Iniciando loop principal de verificação...")
            last_check = datetime.now(timezone.utc)
            while True:
                try:
                    now = datetime.now(timezone.utc)
                    self._check_all_pending_chats()

                    # Verifica chats inativos a cada hora
                    if (now - last_check) > timedelta(hours=12):
                        self._check_inactive_chats()
                        last_check = now
                except Exception as e:
                    logger.error(f"Erro na verificação de chats: {e}")

                time.sleep(self.PENDING_CHECK_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Bot encerrado")
        except Exception as e:
            logger.error(f"Erro fatal no loop principal: {e}")

    def _check_all_pending_chats(self):
        """Verifica todos os chats com mensagens pendentes"""
        try:
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(seconds=self.pending_timeout)

            logger.info(f"Verificando mensagens pendentes (cutoff: {cutoff})")

            query = (
                self.db.collection("pending_messages")
                .where("last_update", "<=", cutoff)
                .where("processing", "==", False)
            )

            docs = query.stream()
            count = 0

            for doc in docs:
                count += 1
                logger.info(f"Processando chat pendente: {doc.id}")
                self._check_pending_messages(doc.id)

            logger.info(f"Total de chats pendentes processados: {count}")

        except Exception as e:
            logger.error(f"Erro na verificação de chats pendentes: {e}", exc_info=True)

bot = WhatsAppGeminiBot()

from threading import Thread
bot_thread = Thread(target=bot.run, daemon=True)
bot_thread.start()

if __name__ == "__main__":
    try:
        bot_thread.join()
    except KeyboardInterrupt:
        logger.info("Bot encerrado")
    except Exception as e:
        logger.error(f"Falha ao iniciar o bot: {e}")
