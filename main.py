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
from datetime import datetime, timedelta

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
    def __init__(self):
        self.reload_env()
        self.db = firestore.Client(project="voola-ai")
        self.pending_timeout = 45  # Timeout para mensagens pendentes (em segundos)
        
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
            'last_update': datetime.now(),
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
        """Armazena o histórico da conversa no Firestore"""
        col_ref = self.db.collection("conversation_history")
        col_ref.add({
            "chat_id": chat_id,
            "message_text": message_text,
            "is_bot": is_bot,
            "timestamp": firestore.SERVER_TIMESTAMP
        })

    def _get_conversation_history(self, chat_id: str, limit: int = 500) -> List[Dict[str, Any]]:
        """Obtém histórico ordenado cronologicamente (corrigido)"""
        try:
            query = (
                self.db.collection("conversation_history")
                .where("chat_id", "==", chat_id)
                .order_by("timestamp", direction=firestore.Query.ASCENDING)
                .limit_to_last(limit)
            )

            # Substitua .stream() por .get()
            docs = query.get()  # Isso resolve o erro

            return [{
                'message_text': doc.get('message_text'),
                'is_bot': doc.get('is_bot'),
                'timestamp': doc.get('timestamp').timestamp() if doc.get('timestamp') else None
            } for doc in docs]

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
            self._save_conversation_history(chat_id, bot_response, True)
            
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
        """Constrói o prompt com histórico formatado corretamente"""
        try:
            history = self._get_conversation_history(chat_id, limit=500)  # Reduzido para melhor performance
            
            if not history:
                return current_prompt

            # Ordena cronologicamente e formata
            sorted_history = sorted(history, key=lambda x: x['timestamp'])
            
            context_str = "\n".join(
                f"{'Usuário' if not msg['is_bot'] else 'Assistente'}: {msg['message_text']}" 
                for msg in sorted_history
            )
            
            return (
                "Histórico da conversa:\n"
                f"{context_str}\n\n"
                "Nova mensagem para responder:\n"
                f"Usuário: {current_prompt}"
            )
            
        except Exception as e:
            logger.error(f"Erro ao construir contexto: {e}")
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
            'timestamp': datetime.now(),
            'message_id': message_id
        })

        # Iniciar verificação de timeout
        self._check_pending_messages(chat_id)

        return None  # Não processa imediatamente
    
    def _check_pending_messages(self, chat_id: str):
        """Verifica se deve processar as mensagens acumuladas"""
        doc_ref = self.db.collection("pending_messages").document(chat_id)
    
        # Função transacional com decorador correto
        @firestore.transactional
        def process_if_ready(transaction):
            doc = doc_ref.get(transaction=transaction)
            if not doc.exists:
                return
    
            data = doc.to_dict()
            if data.get('processing', False):
                return
    
            timeout = (datetime.now() - data['last_update']).total_seconds()
            if timeout >= self.pending_timeout:
                transaction.update(doc_ref, {'processing': True})
    
        try:
            # Cria transação e executa
            transaction = self.db.transaction()
            process_if_ready(transaction)
            self._process_pending_messages(chat_id)
        except Exception as e:
            logger.error(f"Erro na transação: {e}")
            # Reset do estado se necessário
            doc_ref.update({'processing': False})

    def _process_pending_messages(self, chat_id: str):
        """Processa todas as mensagens acumuladas"""
        try:
            data = self._get_pending_messages(chat_id)
            if not data or not data.get('messages'):
                return
    
            # Ordenar mensagens por timestamp
            messages = sorted(data['messages'], key=lambda x: x['timestamp'])
            full_text = "\n".join([msg['text'] for msg in messages])
            message_ids = [msg['message_id'] for msg in messages]
    
            # Gerar resposta
            response_text = self.generate_gemini_response(full_text, chat_id)
    
            # Enviar resposta para a última mensagem
            last_message_id = message_ids[-1]
            self.send_whatsapp_message(chat_id, response_text, last_message_id)
    
            # Atualizar histórico e limpar pendentes
            self.update_conversation_context(chat_id, full_text, response_text)
            self._delete_pending_messages(chat_id)
    
        except Exception as e:
            logger.error(f"Erro ao processar mensagens pendentes: {e}")
            # Resetar processing flag
            doc_ref = self.db.collection("pending_messages").document(chat_id)
            doc_ref.update({'processing': False})

    def generate_gemini_response(self, prompt: str, chat_id: str) -> str:
        """Gera resposta considerando o contexto completo"""
        try:
            full_prompt = self.build_context_prompt(chat_id, prompt)
            response = self.model.generate_content(full_prompt)
            return response.text.strip()
        except Exception as e:
            logger.error(f"Erro no Gemini: {e}")
            return "Desculpe, ocorreu um erro. Por favor, reformule sua pergunta."

    def send_whatsapp_message(self, chat_id: str, text: str, reply_to: str) -> bool:
        """Envia mensagem formatada para o WhatsApp"""
        payload = {
            "to": chat_id,
            "body": text,
            "reply": reply_to
        }

        try:
            response = requests.post(
                "https://gate.whapi.cloud/messages/text",
                headers={
                    "Authorization": f"Bearer {self.whapi_api_key}",
                    "Content-Type": "application/json"
                },
                json=payload
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem: {e}")
            return False


    def run(self):
        """Inicia verificação periódica de mensagens pendentes"""
        try:
            while True:
                self._check_all_pending_chats()
                time.sleep(5)  # Verifica a cada 5 segundos
        except KeyboardInterrupt:
            logger.info("Bot encerrado")

    def _check_all_pending_chats(self):
        """Verifica todos os chats com mensagens pendentes"""
        try:
            now = datetime.now()
            cutoff = now - timedelta(seconds=self.pending_timeout)

            query = (
                self.db.collection("pending_messages")
                .where("last_update", "<=", cutoff)
                .where("processing", "==", False)
            )

            docs = query.stream()
            for doc in docs:
                self._check_pending_messages(doc.id)

        except Exception as e:
            logger.error(f"Erro na verificação de chats pendentes: {e}")

if __name__ == "__main__":
    try:
        bot = WhatsAppGeminiBot()
        bot.run()
    except Exception as e:
        logger.error(f"Falha ao iniciar o bot: {e}")
