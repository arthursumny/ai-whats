import os
import requests
import google.generativeai as genai
from google.genai.types import Tool, GenerateContentConfig, GoogleSearch 
import time
import re
import logging
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
import sqlite3
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

pdf_files = [
    genai.upload_file('zombicide_ptbr.pdf'),
    genai.upload_file('zombicide_en.pdf')
]

class WhatsAppGeminiBot:
    def __init__(self):
        self.reload_env()
        self.db_file = "bot_database.db"
        self._init_db()  # Inicializa o banco de dados
        
        self.processed_message_ids = set()  # Armazena IDs de mensagens já processadas
        self.conversation_contexts = {}  # Armazena o contexto das conversas por chat_id
        self.inactivity_timeout = 600  # 10 minutos em segundos
        self._start_background_cleaner()
        
        if not all([self.whapi_api_key, self.gemini_api_key]):
            raise ValueError("Chaves API não configuradas no .env")
        
        self.setup_apis()

    def _start_background_cleaner(self):
        """Inicia thread para limpar conversas inativas"""
        import threading
        def cleaner_loop():
            while True:
                self._clean_old_conversations()
                time.sleep(60)  # Verifica a cada minuto

        cleaner_thread = threading.Thread(target=cleaner_loop, daemon=True)
        cleaner_thread.start()

    def _init_db(self):
        """Cria as tabelas se não existirem"""
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()

            # Tabela de mensagens processadas (original)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_messages (
                    id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    text_content TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Tabela modificada para histórico de conversas (nova estrutura)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS conversation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT NOT NULL,
                    message_text TEXT NOT NULL,
                    is_bot BOOLEAN NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (chat_id, timestamp)
                )
            """)

            # Cria índice para consultas por chat_id
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_conversation_history_chat_id 
                ON conversation_history (chat_id)
            """)

            conn.commit()

    def _message_exists(self, message_id: str) -> bool:
        """Verifica se a mensagem já foi processada"""
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM processed_messages WHERE id = ?", 
                (message_id,)
            )
            return cursor.fetchone() is not None

    def _save_message(self, message_id: str, chat_id: str, text: str):
        """Armazena a mensagem no banco de dados"""
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO processed_messages 
                   (id, chat_id, text_content) 
                   VALUES (?, ?, ?)""",
                (message_id, chat_id, text)
            )
            conn.commit()

    def _save_conversation_history(self, chat_id: str, message_text: str, is_bot: bool):
        """Armazena o histórico da conversa com tratamento de timestamps duplicados"""
        max_attempts = 3
        attempt = 0

        while attempt < max_attempts:
            try:
                with sqlite3.connect(self.db_file) as conn:
                    cursor = conn.cursor()

                    # Usamos strftime para garantir precisão de milissegundos
                    cursor.execute("""
                        INSERT INTO conversation_history 
                        (chat_id, message_text, is_bot, timestamp)
                        VALUES (?, ?, ?, strftime('%Y-%m-%d %H:%M:%f', 'now'))
                    """, (chat_id, message_text, is_bot))

                    conn.commit()
                    return

            except sqlite3.IntegrityError as e:
                if "UNIQUE" in str(e):
                    attempt += 1
                    if attempt < max_attempts:
                        # Pequeno delay para garantir timestamp único
                        time.sleep(0.1)
                        continue
                logger.error(f"Erro ao salvar histórico: {e}")
                return
            except Exception as e:
                logger.error(f"Erro inesperado ao salvar histórico: {e}")
                return

    def _get_conversation_history(self, chat_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Obtém o histórico de conversa para um chat_id específico"""
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT message_text, is_bot, timestamp 
                   FROM conversation_history 
                   WHERE chat_id = ? 
                   ORDER BY timestamp DESC 
                   LIMIT ?""",
                (chat_id, limit)
            )
            rows = cursor.fetchall()
            
            return [{
                'text': row[0],
                'is_bot': bool(row[1]),
                'timestamp': row[2]
            } for row in rows]

    def _clean_old_conversations(self):
        """Limpa conversas inativas por mais de 10 minutos"""
        current_time = datetime.now()
        inactive_chats = []
        
        for chat_id, context_data in list(self.conversation_contexts.items()):
            last_activity = context_data['last_activity']
            if (current_time - last_activity).total_seconds() > self.inactivity_timeout:
                inactive_chats.append(chat_id)
                # Envia mensagem informando que o contexto foi limpo
                self.send_whatsapp_message(
                    chat_id=chat_id,
                    text="O contexto desta conversa foi encerrado devido a inatividade. Se precisar de ajuda, envie uma nova mensagem.",
                    reply_to=None
                )
                logger.info(f"Contexto limpo para chat_id {chat_id} por inatividade")
        
        # Remove os chats inativos
        for chat_id in inactive_chats:
            del self.conversation_contexts[chat_id]

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
        
        # Atualiza o tempo da última atividade
        self.conversation_contexts[chat_id]['last_activity'] = now
        
        # Salva no histórico
        self._save_conversation_history(chat_id, user_message, False)
        self._save_conversation_history(chat_id, bot_response, True)

    def build_context_prompt(self, chat_id: str, current_prompt: str) -> str:
        """Constrói o prompt com o contexto da conversa"""
        if chat_id not in self.conversation_contexts or not self.conversation_contexts[chat_id]['messages']:
            return current_prompt
        
        context_messages = self.conversation_contexts[chat_id]['messages']
        
        # Limita o contexto para evitar prompt muito longo
        if len(context_messages) > 10:  # Mantém apenas as últimas 5 interações (10 mensagens)
            context_messages = context_messages[-10:]
            self.conversation_contexts[chat_id]['messages'] = context_messages
        
        # Formata o contexto
        context_str = "\n".join(
            f"{msg['role'].capitalize()}: {msg['content']}" 
            for msg in context_messages
        )
        
        return (
            f"Essas são as mensagens anteriores, utilize como contexto para continuar essa conversa:\n"
            f"{context_str}\n\n"
            f"Continue a conversa respondendo a esta mensagem:\n"
            f"Usuário: {current_prompt}"
        )
    
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
        if (message.get('direction') == 'outgoing' or message.get('type') in ['image', 'video', 'document', 'audio', 'sticker']):
            return None
        
        # Verifica se a mensagem já foi processada (banco de dados)
        message_id = message.get('id')
        if not message_id or self._message_exists(message_id):
            return None

        chat_id = message.get('chat_id')
        texto_dict = message.get('text', {})
        texto_original = texto_dict.get('body', '').strip()


        self._save_message(
            message_id=message_id,
            chat_id=chat_id,
            text=texto_original
        )
        return {
            'chat_id': chat_id,
            'texto_original': texto_original,
            'message_id': message_id  # Importante para o reply
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
        mensagem_formatada = f"*BrainEater Guide:*\n\n{text}"

        payload = {
            "to": chat_id,
            "body": mensagem_formatada,
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
        logger.info("Bot aguardando mensagens via webhook...")
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