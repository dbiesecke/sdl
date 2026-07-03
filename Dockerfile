# A multi-stage Dockerfile to build the `sdl-api` binary from the sdl project
# and run it as a lightweight container. The first stage uses the official
# Rust image to compile the project. Dependencies are compiled first to
# leverage Docker layer caching. In the final runtime stage we copy the
# compiled binary and set up the environment for running the HTTP API on
# port 8080.

### Stage 1: Build the project
FROM rust:1.75 as builder

# Install build dependencies and create a working directory
WORKDIR /usr/src/sdl

# Pre-copy Cargo manifest files to leverage Docker layer caching for
# dependencies. A dummy `main.rs` is used to compile the dependencies
# without pulling in the full source yet.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Now copy the full source tree and compile only the `sdl-api` binary.
COPY . .
RUN cargo build --release --bin sdl-api


### Stage 2: Prepare a minimal runtime image
FROM debian:bookworm-slim

# Install CA certificates so `reqwest` with rustls can validate HTTPS
# connections when resolving streams.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/sdl/target/release/sdl-api /usr/local/bin/sdl-api

# Expose the HTTP port and set the binding address. The `SDL_API_BIND`
# variable tells `sdl-api` to listen on all interfaces so that the
# container is reachable from outside. The default in the code binds
# to 127.0.0.1:8080, which would otherwise make the service unreachable.
EXPOSE 8080
ENV SDL_API_BIND=0.0.0.0:8080

# When the container starts it will run the API server on port 8080.
ENTRYPOINT ["sdl-api"]
