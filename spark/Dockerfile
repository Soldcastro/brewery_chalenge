FROM bitnami/spark:3.5.1


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root
RUN mkdir -p /app/scripts && chown -R 1001:1001 /app/scripts
USER 1001