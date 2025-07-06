# Stage 1: Build
FROM rust:1.83.0-slim-bookworm AS build

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    binutils \
    && cargo build --release \
    && strip target/release/payment-processor

# Stage 2: Run
FROM gcr.io/distroless/cc-debian12

WORKDIR /usr/local/bin

COPY --from=build /app/target/release/payment-processor .

ENTRYPOINT ["/usr/local/bin/payment-processor"]