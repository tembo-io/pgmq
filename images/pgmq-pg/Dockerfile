FROM postgres:15.4-bookworm as builder

ARG PGMQ_VER=0.33.1

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
	postgresql-server-dev-15

# Install Rust dependencies
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN $HOME/.cargo/bin/rustup default stable

# init pgrx
RUN $HOME/.cargo/bin/cargo install cargo-pgrx --version=0.11.0 --locked
RUN $HOME/.cargo/bin/cargo pgrx init --pg15 $(which pg_config)

# install pgmq
COPY . .

RUN $HOME/.cargo/bin/cargo pgrx install --pg-config=$(which pg_config)

# install pg_partman
RUN git clone https://github.com/pgpartman/pg_partman.git && \
    cd pg_partman && \
    git checkout v4.7.4 && make && make install

FROM postgres:15.4-bookworm

COPY --from=builder /usr/share/postgresql/15/extension /usr/share/postgresql/15/extension
COPY --from=builder /usr/lib/postgresql/15/lib /usr/lib/postgresql/15/lib

RUN apt-get update \
    && apt-get install -y ca-certificates

COPY ./images/pgmq-pg/postgresql.conf /usr/share/postgresql/15/postgresql.conf.sample

USER postgres
CMD ["postgres"]
