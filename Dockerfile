FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY mock_identity ./mock_identity
COPY runtime ./runtime
RUN pip install --no-cache-dir .
CMD ["uvicorn", "mock_identity.app:create_env_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
