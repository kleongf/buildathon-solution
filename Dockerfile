# Shopping Sessions: native Rust pipeline.
# The scorer builds this natively on linux/amd64 (Intel Xeon, Cascade Lake or
# newer). No FROM line pins a platform, so it also builds natively on arm64.

FROM rust:1.85-slim-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# x86-64-v3 (AVX2/BMI1/BMI2/LZCNT/POPCNT) is available on every Haswell-or-newer
# Xeon, including all scorer CPUs. AVX-512 is never required.
RUN if [ "$TARGETARCH" = "amd64" ] || [ "$(uname -m)" = "x86_64" ]; then \
        export RUSTFLAGS="-C target-cpu=x86-64-v3"; \
    fi; \
    cargo build --release --locked \
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
