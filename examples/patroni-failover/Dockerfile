# Patroni + PostgreSQL for the demo. Patroni publishes no official image, so build one.
FROM postgres:18.4-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-venv python3-pip curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /opt/patroni \
 && /opt/patroni/bin/pip install --no-cache-dir 'patroni[etcd3]' psycopg2-binary \
 && mkdir /pgdata && chown postgres:postgres /pgdata && chmod 700 /pgdata
USER postgres
COPY patroni.yml /etc/patroni.yml
ENTRYPOINT ["/opt/patroni/bin/patroni", "/etc/patroni.yml"]
