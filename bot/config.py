# Intervalo de verificação de mensagens pendentes
PENDING_CHECK_INTERVAL = 2  # segundos

# Timeout para reengajamento de chat inativo
REENGAGEMENT_TIMEOUT = (60 * 60 * 24 * 2)  # 2 dias em segundos

# Mensagens de fallback para reengajamento, caso a geração do Gemini falhe
FALLBACK_REENGAGEMENT_MESSAGES = [
    "Oi! Está tudo bem por aí? Posso ajudar com algo?",
    "Oi! Como posso ajudar você hoje?",
]

# Listas para limpar o conteúdo do lembrete
leading_words_to_strip_normalized = [
    "de", "para", "que", "sobre", "do", "da", "dos", "das",
    "me", "mim", "nos", "pra", "pro", "pros", "pras"
]

trailing_phrases_to_strip_normalized = [
    "as", "às", "hs", "hrs", "horas", "hora",
    "em", "no", "na", "nos", "nas",
    "para", "de", "do", "da", "dos", "das",
    "pelas", "pelos", "a", "o", "amanha",
    "hoje", "la", "lá", "por", "volta",
    "depois", "antes", "proximo", "proxima"
]

# Regex para confirmação de criação de lembrete pelo Gemini
GEMINI_REMINDER_CONFIRMATION_REGEX = r"""(?ix)
(
    # Padrões de confirmação de lembrete
    (?:
        # "lembrete está/foi agendado/criado/anotado"
        lembrete\s+(?:está|esta|foi|será|sera)\s+(?:agendado|criado|anotado|marcado|definido|configurado|certinho|pronto|ok)
        |
        # "agendei/criei/anotei um lembrete"
        (?:agendei|criei|anotei|marquei|defini|configurei)\s+(?:um\s+|o\s+)?lembrete
        |
        # "vou te lembrar/lembrarei"
        (?:vou\s+(?:te\s+)?lembrar|lembrarei|te\s+lembrarei|vou\s+(?:te\s+)?avisar|avisarei)
        |
        # "está certinho/pronto/ok"
        (?:está|esta|tá|ta)\s+(?:confirmado|anotado|agendado)
        |
        # "não esqueça/esquecerei"
        (?:não\s+(?:vou\s+)?esquecer|nao\s+(?:vou\s+)?esquecer|pode\s+deixar|deixa\s+comigo)
        |
        # "lembrete para X às Y"
        lembrete\s+(?:de\s+|para\s+|sobre\s+)?.+?(?:às|as|para)\s+\d{1,2}(?::\d{2})?
        |
        # "te lembro/aviso X"
        te\s+(?:lembro|aviso|alerto|notifico)\s+(?:de\s+|para\s+|sobre\s+)?
        |
        # "anotado para X"
        (?:anotado|agendado|marcado)\s+para
        |
        # "X está/foi agendado"
        .+?\s+(?:está|esta|foi)\s+(?:agendado|anotado|marcado)
    )
)
"""

# Estados da sessão de criação de lembrete
REMINDER_STATE_AWAITING_CONTENT = "awaiting_content"
REMINDER_STATE_AWAITING_DATETIME = "awaiting_datetime"
REMINDER_STATE_AWAITING_RECURRENCE = "awaiting_recurrence"  # Não usado ativamente para perguntar, mas para estado da sessão
REMINDER_STATE_AWAITING_TIME = "awaiting_time"  # Novo estado para quando apenas a hora está faltando
REMINDER_STATE_AWAITING_CANCELLATION_CHOICE = "awaiting_cancellation_choice" # Para fluxo de cancelamento

# Timeouts para sessões de lembrete
REMINDER_SESSION_TIMEOUT_SECONDS = 300  # 5 minutos para sessão de criação de lembrete pendente
REMINDER_CANCELLATION_SESSION_TIMEOUT_SECONDS = 300 # 5 minutos para sessão de cancelamento pendente

# Intervalo para verificar lembretes devidos
REMINDER_CHECK_INTERVAL_SECONDS = 60 # Verificar a cada 60 segundos

# Timezone alvo para o bot
TARGET_TIMEZONE_NAME = 'America/Sao_Paulo'

# Templates para confirmação de lembrete
REMINDER_CONFIRMATION_TEMPLATES = [
    "Claro! Lembrete agendado para {datetime_str}:\n\n*{content}*",
    "Entendido! Seu lembrete para {datetime_str} está configurado:\n\n*{content}*",
    "Anotado! Te lembrarei em {datetime_str} sobre o seguinte:\n\n*{content}*",
    "Perfeito! Lembrete definido para {datetime_str}:\n\n*{content}*",
    "Confirmado! Agendei seu lembrete para {datetime_str}:\n\n*{content}*"
]

# Regex para palavras-chave de cancelamento de lembrete
REMINDER_CANCEL_KEYWORDS_REGEX = r"""(?ix)
    (?:cancelar|cancela|excluir|exclui|remover|remove)\s+
    (?:o\s+|meu\s+|um\s+)?
    (?:lembrete|agendamento)
    (?:\s+de\s+.*|\s+com\s+id\s+\w+)? # Optional: "lembrete de tomar agua" or "lembrete com id X"
    |
    (?:cancelar|cancela|excluir|exclui|remover|remove)\s+
    todos\s+(?:os\s+)?(?:meus\s+)?lembretes
"""

# Dias em português para parsing de data/hora
PORTUGUESE_DAYS_FOR_PARSING = {
    "segunda": "monday", "terça": "tuesday", "quarta": "wednesday",
    "quinta": "thursday", "sexta": "friday", "sábado": "saturday", "domingo": "sunday",
    "segunda-feira": "monday", "terça-feira": "tuesday", "quarta-feira": "wednesday",
    "quinta-feira": "thursday", "sexta-feira": "friday"
}

# Regex para padrão específico de dia mensal (ex: "todo dia 10")
MONTHLY_DAY_SPECIFIC_REGEX = r"""(?ix)
    \b(?:
        (?:todo\s+dia|mensalmente\s+(?:no\s+)?dia|dia) # "todo dia 10", "mensalmente dia 10", "dia 10" (when context implies monthly)
        \s+(\d{1,2})                                  # The day number (1-31)
        (?:
            \s+(?:de\s+cada\s+m[eê]s|por\s+m[eê]s)     # Optional "de cada mes", "por mes"
        )?
    |
        (?:todo\s+m[eê]s|mensalmente)\s+dia\s+(\d{1,2}) # "todo mes dia 10"
    )\b
"""

# Palavras-chave para recorrência de lembretes
RECURRENCE_KEYWORDS = {
    "diariamente": "daily", "todo dia": "daily", "todos os dias": "daily",
    "semanalmente": "weekly", "toda semana": "weekly", "todas as semanas": "weekly",
    "mensalmente": "monthly", "todo mes": "monthly", "todos os meses": "monthly", # "mes" without accent for easier regex
    "anualmente": "yearly", "todo ano": "yearly", "todos os anos": "yearly"
}

# Regex para palavras-chave de solicitação de lembrete (movido para cá, pois é uma constante de configuração)
REMINDER_REQUEST_KEYWORDS_REGEX = r"""(?ix) # Ignore case and allow comments
    \b(?:
        lembrete|
        me\s+lembr(?:a|ar)|
        anota?\s+a[ií]|
        anote\s+a[ií]|
        agend(?:a|ar)\s+um\s+lembrete|
        cria(?:r)?\s+um\s+lembrete|
        preciso\s+de\s+um\s+lembrete|
        quero\s+um\s+lembrete|
        defina\s+um\s+lembrete|
        marcar\s+um\s+lembrete|
        me\s+lembre\s+de|
        lembre-me\s+de|
        me\s+avise\s+de|
        me\s+recorde\s+de
    )\b
"""
