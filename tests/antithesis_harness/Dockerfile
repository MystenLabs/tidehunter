FROM rust:1.90-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang pkg-config wget ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN wget -O /usr/lib/libvoidstar.so https://antithesis.com/assets/instrumentation/libvoidstar.so

ENV RUSTFLAGS="-C target-feature=-crt-static -C codegen-units=1 -C passes=sancov-module \
-C llvm-args=-sanitizer-coverage-level=3 \
-C llvm-args=-sanitizer-coverage-trace-pc-guard \
-C link-args=-Wl,--build-id -Ccodegen-units=1 \
-L/usr/lib/libvoidstar.so -lvoidstar"

RUN cargo fetch --manifest-path tests/antithesis_harness/Cargo.toml
RUN --network=none \
    LD_LIBRARY_PATH=/usr/lib/libvoidstar.so \
    cargo build --frozen --release --manifest-path tests/antithesis_harness/Cargo.toml --features sdk

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/lib/libvoidstar.so /usr/lib/libvoidstar.so
COPY --from=builder /src/tests/antithesis_harness/target/release/antithesis_harness /usr/local/bin/antithesis_harness

ENV TIDEHUNTER_ANTITHESIS_ROOT=/var/lib/tidehunter-antithesis \
    TIDEHUNTER_ANTITHESIS_OPS=100000 \
    RUST_BACKTRACE=1

ENTRYPOINT ["/usr/local/bin/antithesis_harness"]
