import logging
import os # Necessário para os.environ.get, se for usar para configurar o nível de log

def setup_logging(log_level_env_var: str = 'LOG_LEVEL', default_level: str = 'INFO') -> logging.Logger:
    """
    Configura e retorna um objeto logger.

    O nível de log pode ser definido através de uma variável de ambiente.
    """
    log_level_str = os.environ.get(log_level_env_var, default_level).upper()
    log_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    # Define um nível padrão caso o valor da variável de ambiente seja inválido
    level = log_levels.get(log_level_str, logging.INFO)

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler('bot.log', mode='a', encoding='utf-8'), # Adiciona modo 'a' e encoding
            logging.StreamHandler()
        ]
    )

    # Se quiser um logger específico em vez do root logger após basicConfig:
    # logger = logging.getLogger("WhatsAppGeminiBot") # Ou qualquer nome que prefira
    # logger.setLevel(level) # Certifique-se de que o logger específico também tem seu nível definido
    # Se basicConfig já configura o root logger para o nível desejado,
    # e você está satisfeito em usar loggers nomeados que herdam essa configuração,
    # então logging.getLogger(__name__) em outros módulos funcionará com essa config base.

    # Para esta refatoração, vamos retornar um logger nomeado para ser consistente com o uso anterior.
    # O nome __name__ aqui será 'bot.logger_config'. Se quisermos o mesmo nome de antes ('__main__' ou o nome do módulo principal),
    # precisaremos passar o nome como argumento ou usar um nome fixo como "WhatsAppGeminiBot".
    # Vamos usar um nome fixo para o logger principal da aplicação.
    logger = logging.getLogger("WhatsAppGeminiBot")
    # Se basicConfig foi chamado, o logger "WhatsAppGeminiBot" herdará a configuração do root logger.
    # Se você quiser handlers específicos para este logger, adicione-os aqui.
    # No entanto, para este caso, a configuração via basicConfig é suficiente.

    return logger
