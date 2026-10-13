FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    AI_MODEL_CACHE=/models

WORKDIR /app
COPY pyproject.toml ./
COPY botsight_ai ./botsight_ai
RUN pip install .

# The embedding model is baked into the image: the worker starts without internet
# access and texts never leave the infrastructure (Botsight.md, 14.1).
ARG EMBEDDING_MODEL=sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
ENV AI_EMBEDDING_MODEL=${EMBEDDING_MODEL}
RUN python -c "import os; from fastembed import TextEmbedding; TextEmbedding(os.environ['AI_EMBEDDING_MODEL'], cache_dir='/models')"

RUN useradd --uid 10001 --no-create-home app && chown -R app /models
USER app

EXPOSE 8090
ENTRYPOINT ["python", "-m", "botsight_ai"]
CMD ["all"]
