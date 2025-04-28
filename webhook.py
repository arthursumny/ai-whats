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

        messages = data.get('messages', [])
        for message in messages:
            if processed := bot.process_whatsapp_message(message):
                resposta = bot.generate_gemini_response(
                    processed['texto_original'],
                    processed['chat_id']
                )
                bot.send_whatsapp_message(
                    processed['chat_id'],
                    resposta,
                    processed['message_id']
                )

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        logging.error(f"Erro no webhook: {e}")
        return jsonify({'status': 'error'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)  # Render usa porta 10000