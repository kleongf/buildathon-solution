# Shopping Sessions: native Rust pipeline.
# The baseline follows the build architecture. The CSV scanner selects SIMD at runtime.

FROM rust:1.85-slim-bookworm AS build
ARG RUSTFLAGS=""
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked \
    && strip target/release/shopping-sessions \
    && cp target/release/shopping-sessions /shopping-sessions

# Minimal runtime: glibc + libgcc only, no shell. Matches bookworm's glibc.
FROM gcr.io/distroless/cc-debian12
COPY --from=build /shopping-sessions /shopping-sessions
# Scorer env: DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8 MEM_GB=24.
# The binary writes only under OUTPUT_DIR and needs no writable root or /tmp.
ENV DATA_DIR=/data OUTPUT_DIR=/output
USER 1000:1000
ENTRYPOINT ["/shopping-sessions"]
