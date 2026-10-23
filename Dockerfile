FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /srv

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app
COPY loadtest ./loadtest
COPY scripts ./scripts

RUN useradd --create-home --uid 10001 dispatch && chown -R dispatch /srv
USER dispatch

EXPOSE 8000 9102
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status==200 else 1)" || exit 1

# One uvicorn process per container: scale out with more containers, not --workers,
# so /metrics stays accurate per instance. Override CMD with `python -m app.worker` for the worker.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
