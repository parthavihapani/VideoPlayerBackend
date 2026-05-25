FROM python:3.12-slim

# Install ffmpeg (needed by yt-dlp to merge video+audio streams)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Download directory (mountable as a volume)
RUN mkdir -p /tmp/videodl

EXPOSE 8000

CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
