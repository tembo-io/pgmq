FROM rust:1.70-bookworm as builder

ARG TRUNK_VER=0.8.7

RUN cargo install --version $TRUNK_VER pg-trunk

FROM postgres:bookworm

COPY --from=builder /usr/local/cargo/bin/trunk /usr/bin/trunk

ARG PGMQ_VER=0.11.2
RUN apt-get update \
    && apt-get install -y ca-certificates

RUN trunk install pg_partman --version 4.7.3
RUN trunk install pgmq --version $PGMQ_VER

COPY ./postgresql.conf /usr/share/postgresql/15/postgresql.conf.sample

USER postgres
CMD ["postgres"]