# Superset 6.1 (bản "lean": không kèm driver CSDL nào) + driver Trino.
FROM apache/superset:6.1.0

USER root
RUN . /app/.venv/bin/activate && uv pip install --no-cache "trino[sqlalchemy]==0.340.0"
USER superset
