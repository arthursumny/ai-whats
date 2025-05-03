# Use a imagem oficial do Python
FROM python:3.9-slim

# Define o diretório de trabalho
WORKDIR /app

# Copia os arquivos necessários
COPY . .

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Expõe a porta do webhook
EXPOSE 8080

# Comando para iniciar o servidor
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "webhook:app"]