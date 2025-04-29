from datetime import datetime
from flask import Flask, request, jsonify
import logging
from main_turnos_teste import WhatsAppGeminiBot  # Importe sua classe principal


app = Flask(__name__)

# Inicialize o bot
bot = WhatsAppGeminiBot()

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'status': 'Dados inválidos'}), 400

        current_time = datetime.now()

        # Verificação de limpeza corrigida
        if not hasattr(bot, 'last_cleanup'):
            bot.last_cleanup = current_time  # Inicializa como datetime
            
        if (current_time - bot.last_cleanup).total_seconds() > 600:
            bot._clean_old_conversations(current_time)
            bot.last_cleanup = current_time  # Mantém como datetime

        messages = data.get('messages', [])
        for message in messages:
            chat_id = message.get('chat_id')
            
            if chat_id in bot.conversation_contexts:
                last_activity = bot.conversation_contexts[chat_id]['last_activity']
                if isinstance(last_activity, float):  # Conversão de segurança
                    last_activity = datetime.fromtimestamp(last_activity)
                
                if (current_time - last_activity).total_seconds() > bot.inactivity_timeout:
                    bot.send_whatsapp_message(
                        chat_id=chat_id,
                        text="Contexto encerrado por inatividade",
                        reply_to=None
                    )
                    del bot.conversation_contexts[chat_id]

            if message.get('direction') == 'outgoing' or not message.get('text'):
                continue

            if processed := bot.process_whatsapp_message(message):
                resposta = bot.generate_gemini_response(
                    processed['texto_original'],
                    processed['chat_id']
                )
                
                if not resposta.startswith(('*Revora AI:*', '⚠️')):
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
    app.run(host='0.0.0.0', port=10000)  # Render usa porta 10000