from datetime import datetime
import os
from flask import Flask, request, jsonify
import logging
from main import WhatsAppGeminiBot
from google.cloud import firestore

app = Flask(__name__)
bot = WhatsAppGeminiBot()

# Configura um logger específico para health checks
health_logger = logging.getLogger('health')
health_logger.setLevel(logging.WARNING)  # Só loga erros graves

@app.route('/healthz')
def health_check():
    """Endpoint simplificado para Cloud Run"""
    try:
        # Verificação básica do Firestore
        list(bot.db.collection("processed_messages").limit(1).stream())
        return jsonify({'status': 'healthy'}), 200
    except Exception as e:
        health_logger.error(f"Falha no health check: {str(e)}", exc_info=True)
        return jsonify({'status': 'unhealthy'}), 500

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'status': 'Dados inválidos'}), 400

        messages = data.get('messages', [])

        # Processa cada mensagem
        for message in messages:
            chat_id = message.get('chat_id')

            # Get text safely and handle different cases
            text_content = message.get('text')
            
            # If text is a dictionary, try to extract the actual text
            if isinstance(text_content, dict):
                text_content = text_content.get('body') or text_content.get('content') or ''
            
            # Filtro principal - ignora mensagens do bot ou sem texto
            if (str(message.get('from_me', '')).lower() == 'true' or 
                not text_content):
                continue

            # Processa mensagem válida 
            if processed := bot.process_whatsapp_message(message):
                resposta = bot.generate_gemini_response(
                    processed['texto_original'],
                    processed['chat_id']
                )
                
                # Envia resposta (sem verificação redundante)
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))