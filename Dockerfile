# GtmDB API server
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

EXPOSE 8100

# Railway and other hosts set PORT; default 8100 locally.
CMD ["sh", "-c", "exec python -m gtmdb serve --host 0.0.0.0 --port ${PORT:-8100}"]
