from datetime import datetime
import os
from flask import Flask, request, jsonify
import logging
# Importar a classe, não a instância, para criar uma nova se necessário ou usar uma global
from main import WhatsAppGeminiBot # , bot as global_bot_instance (se quiser usar a instância global)

app = Flask(__name__)

# Configuração de logging para o Flask app (pode ser diferente do bot)
if __name__ != '__main__': # Quando rodando com Gunicorn, por exemplo
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

# Usar a instância do bot criada em main.py
# Isso assume que main.py é importado e sua instância 'bot' é acessível.
# Se webhook.py e main.py rodam em processos separados (comum em produção com Gunicorn + worker separado),
# esta abordagem de compartilhar instância não funciona. O webhook apenas enfileiraria tarefas (ex: via Pub/Sub ou Redis)
# e o worker (main.py) as processaria.
# Para a estrutura atual (bot em thread no mesmo processo), isso funciona.
from main import bot # Importa a instância 'bot' de main.py

# Configura um logger específico para health checks
health_logger = logging.getLogger('health')
health_logger.setLevel(logging.WARNING) 

@app.route('/healthz')
def health_check():
    """Endpoint simplificado para Cloud Run"""
    try:
        # Verificação básica do Firestore (usando a instância do bot)
        list(bot.db.collection("processed_messages").limit(1).stream())
        # Testar conexão com Gemini (chamada leve)
        # bot.model.generate_content("health check") # Pode ser custoso/lento para health check
        return jsonify({'status': 'healthy'}), 200
    except Exception as e:
        health_logger.error(f"Falha no health check: {str(e)}", exc_info=True)
        # app.logger.error(f"Falha no health check: {str(e)}", exc_info=True) # Logar no logger do app também
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
            supported_user_content_types = ['text', 'image', 'audio', 'ptt', 'video', 'document'] 
            # 'video' e 'document' serão tratados como texto (caption) por enquanto pela lógica atual do bot.
            
            if msg_type not in supported_user_content_types:
                # Verificar se tem 'text.body' ou 'body', pois alguns eventos podem ter texto.
                # Mas, em geral, se o type não é um tipo de conteúdo, ignorar.
                text_check = (message_payload.get('text', {}).get('body') or 
                              message_payload.get('body') or 
                              message_payload.get('caption'))
                if not text_check and msg_type not in ['image', 'audio', 'ptt']: # Se não for mídia e não tiver texto
                    app.logger.info(f"Webhook: Mensagem tipo '{msg_type}' sem conteúdo de texto claro, ignorando. ID: {message_payload.get('id')}")
                    continue


            # Delega o processamento completo da mensagem (incluindo extração de tipo/conteúdo) ao bot
            # A instância 'bot' é importada de main.py
            try:
                bot.process_whatsapp_message(message_payload)
            except Exception as e_process:
                app.logger.error(f"Erro ao chamar bot.process_whatsapp_message para msg ID {message_payload.get('id')}: {e_process}", exc_info=True)
                # Continuar com outras mensagens se houver

        return jsonify({'status': 'success', 'detail': f'{len(messages_to_process)} mensagens recebidas para processamento.'}), 200
    
    except Exception as e:
        app.logger.error(f"Erro geral no manipulador de webhook: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    # Se este arquivo for executado diretamente, ele iniciará o servidor Flask.
    # A instância 'bot' de main.py (e sua thread) deve ter sido iniciada se main.py foi importado
    # e o bloco if __name__ == "__main__" em main.py executado (o que não acontece se main.py for só importado).
    #
    # Para desenvolvimento, você pode querer rodar o bot e o webhook no mesmo processo:
    # 1. Certifique-se que o `if __name__ == "__main__":` em `main.py` que inicia a thread do bot *não*
    #    execute quando `main.py` é importado por `webhook.py`.
    # 2. Inicie a thread do bot aqui antes de `app.run()`:
    #
    # from threading import Thread
    # if not bot_thread.is_alive(): # Referenciando bot_thread de main.py, precisa de ajuste
    #    logger.info("Iniciando BotWorkerThread a partir do webhook.py...")
    #    bot_worker_thread = Thread(target=bot.run, name="BotWorkerThreadFromWebhook", daemon=True)
    #    bot_worker_thread.start()
    #
    # Contudo, a forma como `main.py` está estruturado (com `bot_thread.start()` no `if __name__`),
    # ao importar `main` em `webhook`, a thread do bot *não* iniciará automaticamente.
    #
    # Solução mais limpa para desenvolvimento local (mesmo processo):
    # Em main.py, mova a inicialização da thread para uma função, ex: `start_bot_worker()`
    # E chame `start_bot_worker()` aqui antes de `app.run()`.
    #
    # Exemplo:
    # if not any(t.name == "BotWorkerThread" for t in threading.enumerate()):
    #     logger.info("Iniciando BotWorkerThread a partir do webhook.py (local dev)...")
    #     bot_thread = Thread(target=bot.run, name="BotWorkerThread", daemon=True)
    #     bot_thread.start()

    app.logger.info("Webhook Flask app iniciando...")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True) # Debug=True para desenvolvimento