FROM python:3.10-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -e .

EXPOSE 8000

CMD ["volveq", "serve"]
