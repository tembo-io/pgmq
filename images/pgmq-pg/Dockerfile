# syntax=docker/dockerfile:1.4

ARG pg_version
FROM postgres:${pg_version}-bookworm as builder

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
	postgresql-server-dev-${pg_version#pg}

# Install Rust dependencies
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN $HOME/.cargo/bin/rustup default stable

# init pgrx
RUN $HOME/.cargo/bin/cargo install cargo-pgrx --version=0.11.0 --locked
RUN $HOME/.cargo/bin/cargo pgrx init --pg${pg_version#pg} $(which pg_config)

# install pgmq
COPY . .

RUN $HOME/.cargo/bin/cargo pgrx install --pg-config=$(which pg_config)

# install pg_partman
RUN git clone https://github.com/pgpartman/pg_partman.git && \
	cd pg_partman && \
	git checkout v4.7.4 && make && make install

FROM postgres:${pg_version}-bookworm

COPY --from=builder /usr/share/postgresql/${pg_version#pg}/extension /usr/share/postgresql/${pg_version#pg}/extension
COPY --from=builder /usr/lib/postgresql/${pg_version#pg}/lib /usr/lib/postgresql/${pg_version#pg}/lib

RUN apt-get update \
	&& apt-get install -y ca-certificates

COPY ./images/pgmq-pg/postgresql.conf /usr/share/postgresql/${pg_version#pg}/postgresql.conf.sample

USER postgres
CMD ["postgres"]
