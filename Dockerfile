FROM public.ecr.aws/lambda/python:3.11

# Copia o arquivo de requisitos para o container
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Instala as dependências
RUN pip install -r requirements.txt

# Copia o código da aplicação para o diretório da Lambda
COPY worker.py ${LAMBDA_TASK_ROOT}

# Define o ponto de entrada para a função de tratamento (arquivo.funcao)
CMD [ "worker.handler" ]