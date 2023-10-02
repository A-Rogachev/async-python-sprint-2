FROM python:3.10.11-slim
WORKDIR /homework
COPY . /homework
CMD ["sh", "-c", "sleep 3600"]