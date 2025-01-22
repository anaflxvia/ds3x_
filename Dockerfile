FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y \
    wget \
    curl \
    unzip \
    ca-certificates \
    chromium-driver \
    libgsf-1-114 \
    libx11-6 \
    libxcomposite1 \
    libxrandr2 \
    libgdk-pixbuf2.0-0 \
    libnss3 \
    libxtst6 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libgdk-pixbuf2.0-0 \
    libdbus-glib-1-2 \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROME_DRIVER=/usr/bin/chromium-driver

RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir 

WORKDIR /app

COPY . /app

ENV GOOGLE_APPLICATION_CREDENTIALS="$HOME/src/credentials.json"

CMD ["python", "main.py"]