from datetime import datetime, timedelta
from flask import Flask, request, jsonify
import logging
from main_turnos_teste import WhatsAppGeminiBot
import os
import requests
import time
from threading import Thread


app = Flask(__name__)

# Inicialize o bot
bot = WhatsAppGeminiBot()

def start_background_tasks():
    """Inicia tarefas em segundo plano para manter o app ativo"""
    def keep_alive():
        while True:
            try:
                # Auto-acionamento a cada 5 minutos
                if os.getenv('ENVIRONMENT') == 'production':
                    requests.get(f"http://localhost:{os.getenv('PORT', '10000')}/healthcheck")
                time.sleep(300)  # 5 minutos
            except Exception as e:
                logging.error(f"Erro no keep-alive: {str(e)}")

    if os.getenv('ENVIRONMENT') == 'production':
        Thread(target=keep_alive, daemon=True).start()

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'status': 'Dados inválidos'}), 400

        current_time = datetime.now()
        messages = data.get('messages', [])

        for message in messages:
            chat_id = message.get('chat_id')
            
            # Verifica inatividade apenas para o chat específico
            if chat_id in bot.conversation_contexts:
                last_activity = bot.conversation_contexts[chat_id]['last_activity']
                if isinstance(last_activity, float):
                    last_activity = datetime.fromtimestamp(last_activity)
                
                if (current_time - last_activity).total_seconds() > bot.inactivity_timeout:
                    bot.send_whatsapp_message(
                        chat_id=chat_id,
                        text="Contexto encerrado por inatividade",
                        reply_to=None
                    )
                    del bot.conversation_contexts[chat_id]

            if str(message.get('from_me')).lower() == 'true' or not message.get('text'):
                continue

            if processed := bot.process_whatsapp_message(message):
                resposta = bot.generate_gemini_response(
                    processed['texto_original'],
                    processed['chat_id']
                )
                
                if not resposta.startswith(('*Revora AI:*')):
                    bot.send_whatsapp_message(
                        processed['chat_id'],
                        resposta,
                        processed['message_id']
                    )
                    bot.update_conversation_context(
                        processed['chat_id'],
                        processed['texto_original'],
                        resposta
                    )

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        logging.error(f"Erro no webhook: {str(e)}", exc_info=True)
        return jsonify({'status': 'error'}), 500

def start_background_tasks():
    def keep_alive():
        while True:
            try:
                # Auto-aciona o webhook a cada 5 minutos
                requests.get(f"http://localhost:{os.getenv('PORT', '10000')}/healthcheck")
                time.sleep(300)
            except Exception as e:
                logging.error(f"Erro no keep-alive: {e}")

    # Inicia a thread quando o app iniciar
    if os.getenv('ENVIRONMENT') == 'production':
        Thread(target=keep_alive, daemon=True).start()

@app.route('/healthcheck')
def healthcheck():
    try:
        now = datetime.now()
        # Limpeza a cada 10 minutos
        if (now - bot.last_cleanup).total_seconds() > 600:
            bot._clean_old_conversations(now)
            bot.last_cleanup = now
            logging.info("Limpeza automática executada via healthcheck")
        
        return jsonify({
            'status': 'active',
            'last_cleanup': bot.last_cleanup.isoformat()
        }), 200
    except Exception as e:
        logging.error(f"Healthcheck falhou: {str(e)}")
        return jsonify({'status': 'error'}), 500

# Inicia as tarefas em segundo plano
start_background_tasks()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)