# syntax=docker/dockerfile:1.4

ARG PG_DOCKER_VERSION
FROM postgres:${PG_DOCKER_VERSION}-bookworm as builder
ARG PG_DOCKER_VERSION

RUN apt-get update \
	&& apt-get install -y \
	ca-certificates \
	clang \
	curl \
	gcc \
	git \
	libssl-dev \
	make \
	pkg-config \
	postgresql-server-dev-${PG_DOCKER_VERSION}

# Install Rust dependencies
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN $HOME/.cargo/bin/rustup default stable

# init pgrx
RUN $HOME/.cargo/bin/cargo install cargo-pgrx --version=0.11.0 --locked
RUN $HOME/.cargo/bin/cargo pgrx init --pg${PG_DOCKER_VERSION} $(which pg_config)

# install pgmq
COPY . .

RUN $HOME/.cargo/bin/cargo pgrx install --pg-config=$(which pg_config)

# install pg_partman
RUN git clone https://github.com/pgpartman/pg_partman.git && \
	cd pg_partman && \
	git checkout v4.7.4 && make && make install

ARG PG_DOCKER_VERSION=${PG_DOCKER_VERSION}
FROM postgres:${PG_DOCKER_VERSION}-bookworm
ARG PG_DOCKER_VERSION

COPY --from=builder /usr/share/postgresql/${PG_DOCKER_VERSION}/extension /usr/share/postgresql/${PG_DOCKER_VERSION}/extension
COPY --from=builder /usr/lib/postgresql/${PG_DOCKER_VERSION}/lib /usr/lib/postgresql/${PG_DOCKER_VERSION}/lib

RUN apt-get update \
	&& apt-get install -y ca-certificates

COPY ./images/pgmq-pg/postgresql.conf /usr/share/postgresql/${PG_DOCKER_VERSION}/postgresql.conf.sample

USER postgres
CMD ["postgres"]
