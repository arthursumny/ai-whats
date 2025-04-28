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
        
        bot._clean_old_conversations()

        messages = data.get('messages', [])
        for message in messages:
            chat_id = message.get('chat_id')
            # Filtra mensagens enviadas pelo bot (direção "outgoing" ou sem 'text')
            if chat_id in bot.conversation_contexts:
                last_activity = bot.conversation_contexts[chat_id]['last_activity']
                inactivity_period = (datetime.now() - last_activity).total_seconds()
                if inactivity_period > bot.inactivity_timeout:
                    bot.send_whatsapp_message(
                        chat_id=chat_id,
                        text="O contexto desta conversa foi encerrado devido a inatividade. Envie uma nova mensagem para continuar.",
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
                # Evita responder a si mesmo (opcional)
                if not resposta.startswith(('*BrainEater Guide:*', '⚠️')):
                    bot.send_whatsapp_message(
                        processed['chat_id'],
                        resposta,
                        processed['message_id']
                    )
                    # Atualiza o contexto e histórico no banco de dados
                    bot.update_conversation_context(
                        processed['chat_id'],
                        processed['texto_original'],
                        resposta
                    )

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        logging.error(f"Erro no webhook: {e}")
        return jsonify({'status': 'error'}), 500
    
from flask import abort
import os

@app.route('/cron/cleanup', methods=['GET'])
def cleanup_old_conversations():
    try:
        # Verifica token
        if request.args.get('token') != os.getenv('CLEANUP_TOKEN'):
            abort(401)
            
        bot._clean_old_conversations()
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        logging.error(f"Erro na limpeza: {e}")
        return jsonify({'status': 'error'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)  # Render usa porta 10000