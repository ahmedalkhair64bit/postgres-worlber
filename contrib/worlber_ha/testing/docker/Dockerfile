# Worlber PostgreSQL + worlber_ha test image.
#
# Build from the repository root (docker compose does this for you):
#   docker build -f contrib/worlber_ha/testing/docker/Dockerfile -t worlber-pg:lab .
#
# Behind a TLS-intercepting proxy, pass its CA certificate as a build secret:
#   --secret id=ca,src=/path/to/ca.crt

# --- agent ------------------------------------------------------------------
FROM golang:1.24-bookworm AS agent
RUN --mount=type=secret,id=ca,required=false \
    if [ -s /run/secrets/ca ]; then cp /run/secrets/ca /usr/local/share/ca-certificates/extra.crt && update-ca-certificates; fi
WORKDIR /src
COPY contrib/worlber_ha/agent/go.mod contrib/worlber_ha/agent/go.sum ./
RUN GOTOOLCHAIN=local go mod download
COPY contrib/worlber_ha/agent/ ./
RUN CGO_ENABLED=0 GOTOOLCHAIN=local go build -trimpath -o /worlber-ha-agent ./cmd/worlber-ha-agent

# --- PostgreSQL --------------------------------------------------------------
FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential bison flex perl pkg-config \
        libreadline-dev zlib1g-dev libssl-dev liblz4-dev libzstd-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . /src
WORKDIR /build
RUN /src/configure --prefix=/usr/local/pgsql --with-openssl --with-lz4 --with-zstd --without-icu \
        --with-system-tzdata=/usr/share/zoneinfo >/dev/null \
    && make -j"$(nproc)" -s \
    && make -s install \
    && make -s -C contrib install

# --- runtime -----------------------------------------------------------------
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        libreadline8t64 zlib1g libssl3t64 liblz4-1 libzstd1 tzdata ca-certificates \
        procps iproute2 less \
    && rm -rf /var/lib/apt/lists/* \
    && (getent passwd postgres || useradd --system --home-dir /var/lib/postgresql --create-home --shell /bin/bash postgres)
COPY --from=build /usr/local/pgsql /usr/local/pgsql
COPY --from=agent /worlber-ha-agent /usr/local/pgsql/bin/worlber-ha-agent
COPY contrib/worlber_ha/testing/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 755 /usr/local/bin/entrypoint.sh
ENV PATH=/usr/local/pgsql/bin:$PATH \
    PGDATA=/var/lib/postgresql/data \
    PGHOST=/var/run/postgresql \
    PGUSER=postgres
EXPOSE 5432 7400 7401
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
