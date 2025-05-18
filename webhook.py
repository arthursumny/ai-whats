from datetime import datetime
import os
from flask import Flask, request, jsonify
import logging
from threading import Thread # Importar Thread
import time # Para checagem da thread
from main import WhatsAppGeminiBot, bot # , bot as global_bot_instance (se quiser usar a instância global)

app = Flask(__name__)

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
else: # Configuração básica para execução local direta de webhook.py
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ----- INÍCIO: LÓGICA PARA INICIAR A THREAD DO BOT -----
bot_worker_thread = None

def start_bot_worker_if_not_running():
    global bot_worker_thread
    # Verifica se a thread já foi criada e está viva
    if bot_worker_thread is None or not bot_worker_thread.is_alive():
        app.logger.info("Iniciando BotWorkerThread a partir do webhook.py...")
        bot_worker_thread = Thread(target=bot.run, name="BotWorkerThread", daemon=True)
        bot_worker_thread.start()
        if bot_worker_thread.is_alive():
            app.logger.info("BotWorkerThread iniciada com sucesso.")
        else:
            app.logger.error("Falha ao iniciar BotWorkerThread.")
    else:
        app.logger.info("BotWorkerThread já está em execução.")

# Chama a função para iniciar a thread do bot quando este módulo é carregado.
# Isso é executado uma vez quando o Gunicorn (ou similar) carrega o app.
start_bot_worker_if_not_running()
# ----- FIM: LÓGICA PARA INICIAR A THREAD DO BOT -----

# Configura um logger específico para health checks
health_logger = logging.getLogger('health')
health_logger.setLevel(logging.WARNING) 

@app.route('/healthz')
def health_check():
    global bot_worker_thread
    try:
        list(bot.db.collection("processed_messages").limit(1).stream())
        # Verificar se a thread do bot está viva
        if bot_worker_thread is None or not bot_worker_thread.is_alive():
            health_logger.error("Health check falhou: BotWorkerThread não está ativa.")
            return jsonify({'status': 'unhealthy', 'reason': 'Bot worker thread not running'}), 500
        return jsonify({'status': 'healthy'}), 200
    except Exception as e:
        health_logger.error(f"Falha no health check: {str(e)}", exc_info=True)
        return jsonify({'status': 'unhealthy', 'reason': str(e)}), 500

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    try:
        data = request.get_json()
        if not data:
            app.logger.warning("Webhook recebeu dados inválidos ou vazios.")
            return jsonify({'status': 'Dados inválidos'}), 400

        # Whapi pode enviar uma única mensagem ou uma lista em 'messages'
        # Ou, às vezes, a mensagem está no corpo raiz do POST (menos comum para Whapi)
        
        messages_to_process = []
        if 'messages' in data and isinstance(data['messages'], list):
            messages_to_process = data['messages']
        elif 'message' in data and isinstance(data['message'], dict): # Algumas APIs enviam 'message'
             messages_to_process = [data['message']]
        elif 'id' in data and 'chat_id' in data: # Se a mensagem for o próprio corpo do JSON
            messages_to_process = [data]
        else:
            app.logger.warning(f"Webhook: formato de mensagem não esperado. Dados: {data}")
            return jsonify({'status': 'Formato de mensagem não esperado'}), 400


        app.logger.info(f"Webhook recebeu {len(messages_to_process)} mensagem(ns).")

        for message_payload in messages_to_process:
            # Filtro principal: Ignorar mensagens enviadas pelo próprio bot ('from_me' == true)
            # Whapi usa strings 'true'/'false' ou booleans para from_me.
            from_me_val = message_payload.get('from_me', False)
            if str(from_me_val).lower() == 'true':
                app.logger.info(f"Webhook: Mensagem de {message_payload.get('chat_id')} é do bot (from_me=true), ignorando.")
                continue
            
            # Ignorar tipos de mensagem que não são de usuário (eventos de grupo, etc.)
            # Whapi 'type' pode ser 'text', 'image', 'audio', 'video', 'document', 'ptt', 
            # mas também 'event', 'notification', 'call_log', etc.
            # Focaremos nos tipos que carregam conteúdo do usuário.
            msg_type = message_payload.get('type', 'unknown')
            supported_user_content_types = ['text', 'image', 'audio', 'ptt', 'video', 'document', 'voice'] 
            # 'video' e 'document' serão tratados como texto (caption) por enquanto pela lógica atual do bot.
            
            if msg_type not in supported_user_content_types:
                # Verificar se tem 'text.body' ou 'body', pois alguns eventos podem ter texto.
                # Mas, em geral, se o type não é um tipo de conteúdo, ignorar.
                text_check = (message_payload.get('text', {}).get('body') or 
                              message_payload.get('body') or 
                              message_payload.get('caption'))
                if not text_check and msg_type not in ['image', 'audio', 'ptt', 'voice']: # Se não for mídia e não tiver texto
                    app.logger.info(f"Webhook: Mensagem tipo '{msg_type}' sem conteúdo de texto claro, ignorando. ID: {message_payload.get('id')}")
                    continue


            # Delega o processamento completo da mensagem (incluindo extração de tipo/conteúdo) ao bot
            # A instância 'bot' é importada de main.py
            try:
                bot.process_whatsapp_message(message_payload)
            except Exception as e_process:
                app.logger.error(f"Erro ao chamar bot.process_whatsapp_message para msg ID {message_payload.get('id')}: {e_process}", exc_info=True)
                # Continuar com outras mensagens se houver

            try:
                app.logger.info("Webhook processou as mensagens. Iniciando limpeza global de mensagens de bot do histórico...")
                bot.delete_all_bot_messages_globally()
                app.logger.info("Limpeza global de mensagens de bot do histórico concluída após o processamento do webhook.")
            except Exception as e_global_delete:
                # Loga o erro, mas não impede a resposta de sucesso do webhook para as mensagens processadas.
                app.logger.error(f"Erro durante a execução da limpeza global de mensagens de bot: {e_global_delete}", exc_info=True)

                return jsonify({'status': 'success', 'detail': f'{len(messages_to_process)} mensagens recebidas para processamento.'}), 200
    
    except Exception as e:
        app.logger.error(f"Erro geral no manipulador de webhook: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    # Este bloco é para rodar o Flask localmente com 'python webhook.py'
    # A lógica de start_bot_worker_if_not_running() já terá sido chamada na carga do módulo.
    app.logger.info("Webhook Flask app iniciando para desenvolvimento local...")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)