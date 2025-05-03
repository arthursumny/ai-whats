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
        self.db = firestore.Client()
        
        self.processed_message_ids = set()  
        self.conversation_contexts = {}  
        
        if not all([self.whapi_api_key, self.gemini_api_key]):
            raise ValueError("Chaves API não configuradas no .env")
        
        self.setup_apis()
        self.last_cleanup = datetime.now()



    def _message_exists(self, message_id: str) -> bool:
        """Verifica se a mensagem já foi processada (Firestore)"""
        doc_ref = self.db.collection("processed_messages").document(message_id)
        return doc_ref.get().exists

    def _save_message(self, message_id: str, chat_id: str, text: str):
        """Armazena a mensagem no Firestore"""
        doc_ref = self.db.collection("processed_messages").document(message_id)
        doc_ref.set({
            "chat_id": chat_id,
            "text_content": text,
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

    def _get_conversation_history(self, chat_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Obtém o histórico de conversa do Firestore"""
        query = (
            self.db.collection("conversation_history")
            .where(filter=FieldFilter("chat_id", "==", chat_id))
            .order_by("timestamp", direction=firestore.Query.DESCENDING)
            .limit(limit)
        )
        
        return [{
            'message_text': doc.get('message_text'),
            'is_bot': doc.get('is_bot'),
            'timestamp': doc.get('timestamp')
        } for doc in query.stream()]

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
        """Atualiza o contexto da conversa para um chat_id específico"""
        now = datetime.now()
        
        if chat_id not in self.conversation_contexts:
            self.conversation_contexts[chat_id] = {
                'messages': [],
                'last_activity': now
            }
        
        # Adiciona a mensagem do usuário e a resposta do bot ao contexto
        self.conversation_contexts[chat_id]['messages'].extend([
            {'role': 'user', 'content': user_message},
            {'role': 'assistant', 'content': bot_response}
        ])
        
        # Salva no histórico
        self._save_conversation_history(chat_id, user_message, False)
        self._save_conversation_history(chat_id, bot_response, True)

    def build_context_prompt(self, chat_id: str, current_prompt: str) -> str:
        """Constrói o prompt usando o histórico do banco de dados"""
        try:
            # Busca histórico do banco
            history = self._get_conversation_history(chat_id, limit=50)

            if not history:
                return current_prompt

            # Formata o contexto
            context_str = "\n".join(
                f"{'Usuário' if not row['is_bot'] else 'Assistente'}: {row['message_text']}" 
                for row in reversed(history)  # Inverte para ordem cronológica correta
            )
        
            return (
                f"Essas são as mensagens anteriores, utilize como contexto para continuar essa conversa, se necessário:\n"
                f"{context_str}\n\n"
                f"Continue a conversa respondendo a esta mensagem:\n"
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
        """Filtra mensagens conforme regras e prepara para processamento"""
        logger.info(f"Mensagem recebida: {message}")

        # Verifica se a mensagem já foi processada (banco de dados)
        message_id = message.get('id')
        if not message_id or self._message_exists(message_id):
            return None

        chat_id = message.get('chat_id')
        texto_original = message.get('text', {}).get('body', '').strip()

        self._save_message(
            message_id=message_id,
            chat_id=chat_id,
            text=texto_original
        )
        return {
            'chat_id': chat_id,
            'texto_original': texto_original,
            'message_id': message_id
        }
    def generate_gemini_response(self, prompt: str, chat_id: str, pdf_paths: list = None) -> str:
        """Gera resposta usando Gemini com contexto da conversa"""
        try:
            # Adiciona contexto da conversa se existir
            full_prompt = self.build_context_prompt(chat_id, prompt)
            
            if not pdf_paths:
                response = self.model.generate_content(full_prompt)
                return response.text.strip()
            
            # Prepara o conteúdo com a estrutura correta
            content = {"parts": [full_prompt]}
            
            uploaded_files = []
            for path in pdf_paths:
                file = genai.upload_file(path)
                uploaded_files.append(file)
                content["parts"].append({
                    "file": file,
                    "mime_type": "application/pdf"
                })
            
            # Envia a requisição
            response = self.model.generate_content(content)
            
            # Limpeza
            for file in uploaded_files:
                genai.delete_file(file.name)
                
            return response.text.strip()
        
        except Exception as e:
            logger.error(f"Erro no Gemini: {e}")
            return "Desculpe, ocorreu um erro ao processar sua mensagem. Por favor, tente novamente."

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
        """Aguarda mensagens via webhook (não faz mais polling)"""
        try:
            while True:
                time.sleep(1)  # Mantém o script vivo sem consumir CPU
        except KeyboardInterrupt:
            logger.info("Bot encerrado")

if __name__ == "__main__":
    try:
        bot = WhatsAppGeminiBot()
        bot.run()
    except Exception as e:
        logger.error(f"Falha ao iniciar o bot: {e}")
