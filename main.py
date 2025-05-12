import os
import requests
import google.generativeai as genai
from google.genai.types import Tool, GenerationConfig, GoogleSearch # GenerateContentConfig já existe
import time
import re
import logging
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from datetime import datetime, timedelta, timezone
# import tempfile # Não mais necessário com upload via stream
# import shutil   # Não mais necessário
# import mimetypes # Para fallback de mimetype, mas idealmente Whapi fornece

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
    REENGAGEMENT_TIMEOUT = 43200  # 12 horas em segundos
    # REENGAGEMENT_MESSAGES não será mais usado para a lógica principal,
    # mas pode ser um fallback se a geração do Gemini falhar.
    FALLBACK_REENGAGEMENT_MESSAGES = [
        "Oi! Está tudo bem por aí? Posso ajudar com algo?",
        "Oi! Como posso ajudar você hoje?",
    ]
    def __init__(self):
        self.reload_env()
        self.db = firestore.Client(project="voola-ai") # Seu projeto
        self.pending_timeout = 20  # Timeout para mensagens pendentes (em segundos)
        
        if not all([self.whapi_api_key, self.gemini_api_key]):
            raise ValueError("Chaves API não configuradas no .env")
        
        self.setup_apis()

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
            docs = query.get() # Use stream() para iterar

            history = []
            for doc in docs:
                data = doc.to_dict()
                if 'message_text' in data:
                    history.append({
                        'message_text': data['message_text'],
                        'is_bot': data.get('is_bot', False), # Adicionado
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
        self.gemini_model_name = os.getenv('GEMINI_MODEL') # Renomeado para clareza
        self.gemini_context = os.getenv('GEMINI_CONTEXT', '').replace('\\n', '\n')
        
    def setup_apis(self):
        """Configura as conexões com as APIs"""
        try:
            genai.configure(api_key=self.gemini_api_key)
            # Configura ferramenta de busca na web
            self.search_tool = Tool(google_search=GoogleSearch())

            self.model = genai.GenerativeModel(
                model_name=self.gemini_model_name,
                system_instruction=self.gemini_context,
                # Tools podem ser passadas aqui ou em cada chamada generate_content.
                # Passar em cada chamada é mais flexível se nem todas precisarem.
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
            self._save_conversation_history(chat_id, bot_response, True)  # Resposta do bot
            
            context_ref = self.db.collection("conversation_contexts").document(chat_id)
            context_ref.set({
                "last_updated": firestore.SERVER_TIMESTAMP,
                "last_user_message": user_message, # O user_message aqui é o texto consolidado
                "last_bot_response": bot_response
            }, merge=True)
        except Exception as e:
            logger.error(f"Erro ao atualizar contexto: {e}")

    def build_context_prompt(self, chat_id: str, current_prompt_text: str) -> str:
        """Constrói o prompt com histórico formatado corretamente, incluindo o resumo."""
        try:
            summary_ref = self.db.collection("conversation_summaries").document(chat_id)
            summary_doc = summary_ref.get()
            summary = summary_doc.get("summary") if summary_doc.exists else ""

            history = self._get_conversation_history(chat_id, limit=50) # Limite menor para prompt

            if not history and not summary:
                return f"Usuário: {current_prompt_text}" # Adiciona prefixo Usuário

            # Ordenar cronologicamente já é feito por _get_conversation_history
            context_parts = []
            for msg in history:
                role = "Usuário" if not msg.get('is_bot', False) else "Assistente"
                context_parts.append(f"{role}: {msg['message_text']}")
            context_str = "\n".join(context_parts)
            
            # Monta o prompt final
            final_prompt = []
            if summary:
                final_prompt.append(f"### Resumo da conversa anterior ###\n{summary}\n")
            if context_str:
                final_prompt.append(f"### Histórico recente da conversa ###\n{context_str}\n")
            
            final_prompt.append("### Nova interação ###")
            final_prompt.append(f"Usuário: {current_prompt_text}") # current_prompt_text já pode conter descrições de mídia
            
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
        """
        Processa a mensagem do WhatsApp, identifica tipo, extrai conteúdo
        e salva como mensagem pendente.
        """
        logger.info(f"Raw mensagem recebida: {message}")

        message_id = message.get('id')
        if not message_id:
            logger.warning("Mensagem sem ID recebida, ignorando.")
            return

        if self._message_exists(message_id):
            logger.info(f"Mensagem {message_id} já processada, ignorando.")
            return

        chat_id = message.get('chat_id')
        from_name = message.get('from_name', 'Desconhecido')
        
        # Extração de conteúdo
        msg_type_whapi = message.get('type', 'text')
        text_body = None
        media_url = message.get('media') # Campo primário para URL de mídia na Whapi
        caption = message.get('caption')
        mimetype = message.get('mimetype')

        if 'text' in message and isinstance(message['text'], dict): # Estrutura comum para texto
            text_body = message['text'].get('body', '')
        elif 'body' in message and isinstance(message['body'], str): # Whapi às vezes manda body direto
             text_body = message.get('body', '')
        
        # Se media_url estiver no body (caso raro, mas para robustez)
        if not media_url and text_body and msg_type_whapi != 'text' and ('http://' in text_body or 'https://' in text_body):
            # Heurística: se o tipo não é texto mas o body parece uma URL, pode ser a URL da mídia
            # Isso é menos confiável, Whapi deveria usar o campo 'media'
            logger.warning(f"Media URL encontrada no 'body' para msg_type {msg_type_whapi}. Usando-a.")
            media_url = text_body # Assume que o corpo é a URL da mídia
            # Se o body era a URL, não há 'texto' real a menos que haja caption.
            text_body = "" # Limpa text_body se ele foi interpretado como media_url

        # Determinar o tipo processado internamente e o conteúdo principal
        processed_type_internal = 'text'
        content_to_store = text_body if text_body else "" # Default para texto

        if media_url:
            if msg_type_whapi == 'image':
                processed_type_internal = 'image'
                content_to_store = media_url
            elif msg_type_whapi in ['audio', 'ptt']: # ptt é Push-to-talk (áudio)
                processed_type_internal = 'audio'
                content_to_store = media_url
            # Outros tipos (video, document) poderiam ser adicionados.
            # Por agora, se tiverem media_url mas não forem image/audio,
            # serão tratados como 'text' usando seu caption (se houver).
            elif caption: # Se for outro tipo de mídia com caption
                 content_to_store = caption # O "texto" é o caption
                 logger.info(f"Mídia tipo {msg_type_whapi} com caption, tratando como texto '{caption}'. Mídia URL: {media_url}")
            else: # Outro tipo de mídia sem caption
                 logger.info(f"Mídia tipo {msg_type_whapi} sem caption, ignorando mídia por ora. URL: {media_url}")
                 # Se não há caption, e não é imagem/audio, não há o que processar como texto inicial.
                 # O _save_message abaixo irá registrar o tipo original da Whapi.
                 pass # content_to_store permanece como string vazia se text_body era vazio
        
        # Texto para salvar no log de mensagens processadas (pode ser caption ou descrição futura)
        # Inicialmente, é o texto ou legenda. Será atualizado se for mídia e for descrita.
        text_for_processed_log = caption if media_url and caption else text_body if text_body else f"[{processed_type_internal} recebida]"

        self._save_message(message_id, chat_id, text_for_processed_log, from_name, msg_type_whapi)

        # Validar se há algo para colocar na fila pendente
        if processed_type_internal == 'text' and not content_to_store.strip():
            logger.info(f"Mensagem de texto vazia ou mídia não suportada sem caption para {chat_id}, ignorando adição à fila pendente.")
            return
        if processed_type_internal != 'text' and not media_url:
            logger.warning(f"Tipo {processed_type_internal} esperado mas sem media_url. Tentando tratar como texto: '{caption or text_body}'")
            processed_type_internal = 'text'
            content_to_store = caption or text_body or ""
            if not content_to_store.strip():
                logger.info(f"Após fallback para texto, conteúdo ainda vazio para {chat_id}, ignorando.")
                return
        
        pending_payload = {
            'type': processed_type_internal,    # 'text', 'audio', 'image'
            'content': content_to_store,         # texto original ou media_url
            'original_caption': caption,         # caption original da mídia
            'mimetype': mimetype,                # mimetype original da Whapi
            'timestamp': datetime.now(timezone.utc).isoformat(), # Use ISO format para Firestore
            'message_id': message_id
        }
        self._save_pending_message(chat_id, pending_payload)
        # self._check_pending_messages(chat_id) # A verificação agora é feita pelo loop principal
        logger.info(f"Mensagem de {from_name} ({chat_id}) adicionada à fila pendente. Tipo: {processed_type_internal}.")
        return

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
                elif msg_type in ['audio', 'image']:
                    media_url = content
                    if not mimetype:
                        # Tentar inferir mimetype da URL como último recurso (pouco confiável)
                        # Idealmente, Whapi sempre envia mimetype.
                        try:
                            file_ext = os.path.splitext(media_url.split('?')[0])[1].lower() # Remove query params
                            if file_ext == ".jpg" or file_ext == ".jpeg": mimetype = "image/jpeg"
                            elif file_ext == ".png": mimetype = "image/png"
                            elif file_ext == ".mp3": mimetype = "audio/mpeg"
                            elif file_ext == ".ogg": mimetype = "audio/ogg" # Comum para PTT
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

                        display_name = f"media_{chat_id}_{msg_data['message_id']}"
                        # Upload para Gemini
                        file_part_uploaded = genai.upload_file(
                            path=media_response.raw, 
                            display_name=display_name,
                            mime_type=mimetype
                        )
                        logger.info(f"Mídia {file_part_uploaded.name} ({file_part_uploaded.uri}) enviada para Gemini.")
                    
                        prompt_for_media = "Descreva este arquivo de forma concisa e objetiva."
                        if msg_type == 'audio':
                            prompt_for_media = "Transcreva este áudio. Se não for possível transcrever, descreva o conteúdo do áudio de forma concisa."
                        
                        # Gerar descrição/transcrição
                        media_desc_response = self.model.generate_content(
                            [prompt_for_media, file_part_uploaded], # Lista de [texto, arquivo]
                            request_options={'timeout': 180} # Timeout maior para processamento de mídia
                        )
                        media_description = media_desc_response.text.strip()
                        
                        entry = f"Usuário enviou um(a) {msg_type}"
                        if original_caption:
                            entry += f" com a legenda '{original_caption}'"
                        entry += f": [Conteúdo processado da mídia: {media_description}]"
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

            logger.info(f"Texto consolidado para Gemini ({chat_id}): {full_user_input_text[:200]}...")
            
            # Gerar resposta do Gemini
            response_text = self.generate_gemini_response(full_user_input_text, chat_id)
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
                "O usuário deste chat não interage há algum tempo (cerca de 12 horas ou mais).\n"
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
            reengagement_response = self.model.generate_content(
                full_reengagement_prompt,
                # Safety settings podem ser ajustados se necessário para este tipo de prompt
                # generation_config=genai.types.GenerationConfig(temperature=0.7)
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

    def generate_gemini_response(self, current_input_text: str, chat_id: str) -> str:
        """Gera resposta do Gemini considerando o contexto completo e usando Google Search tool."""
        try:
            # current_input_text é o texto já processado (incluindo descrições de mídia)
            full_prompt_with_history = self.build_context_prompt(chat_id, current_input_text)
            
            logger.info(f"Prompt final para Gemini (chat {chat_id}): {full_prompt_with_history[:500]}...")

            # Configuração de geração, se precisar de mais controle (temperatura, etc.)
            config = GenerationConfig(
                tools=[self.search_tool],
            )

            response = self.model.generate_content(
                contents=[full_prompt_with_history], # `contents` deve ser uma lista
                generation_config=config,   # Ativa a ferramenta de pesquisa
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
                      logger.info(f"Gemini usou Google Search. Query: {search_entry.rendered_content if search_entry else 'N/A'}")


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
            response = self.model.generate_content(summary_prompt)
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
                    
                    # 3. Outras tarefas de manutenção (resumo é chamado no _process_pending_messages)

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