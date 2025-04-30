from datetime import datetime
from flask import Flask, request, jsonify
import logging
from main_turnos_teste import WhatsAppGeminiBot

app = Flask(__name__)
bot = WhatsAppGeminiBot()

# Configura um logger específico para health checks
health_logger = logging.getLogger('health')
health_logger.setLevel(logging.WARNING)  # Só loga erros graves

@app.route('/healthz')
def health_check():
    """Endpoint silencioso para health checks"""
    try:
        # Verificação rápida do BD (sem logs em caso de sucesso)
        with bot._get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
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

        current_time = datetime.now()
        messages = data.get('messages', [])

        # Processa cada mensagem
        for message in messages:
            chat_id = message.get('chat_id')
            
            # Filtro principal - ignora mensagens do bot ou sem texto ou se nao comecarem com a palavra Revora AI
            if str(message.get('from_me')).lower() == 'true' or not message.get('text') or not message.get('text').startswith('Revora AI'):
                continue

            # Verifica inatividade do chat específico
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

        # Limpeza geral do BD
        if messages:
            bot._clean_old_conversations(current_time, cleanup_db=True)

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        logging.error(f"Erro no webhook: {str(e)}", exc_info=True)
        return jsonify({'status': 'error'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)