FROM python:3.12-slim

LABEL maintainer="github.com/hooperstu"
LABEL description="The Crawl Street Journal — web crawler and estate inventory tool"

WORKDIR /app

# System dependencies for lxml
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends \
       libxml2 libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Projects and crawl data persist in this volume
VOLUME ["/app/projects"]

EXPOSE 5001

ENV PYTHONUNBUFFERED=1

# Run the Flask GUI directly (not the desktop launcher — no browser/window needed)
CMD ["python3", "gui.py"]
