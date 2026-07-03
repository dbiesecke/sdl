# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.96
ARG SDL_REPO=https://github.com/dbiesecke/sdl.git
ARG SDL_REF=main

### Stage 1: Build the project
FROM rust:${RUST_VERSION}-bookworm AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      git \
      pkg-config && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/sdl

ARG SDL_REPO
ARG SDL_REF

# Fetch the sdl source from GitHub. SDL_REF can be a branch, tag, or ref.
RUN git init . && \
    git remote add origin "${SDL_REPO}" && \
    git fetch --depth 1 origin "${SDL_REF}" && \
    git checkout FETCH_HEAD

# Compile only the sdl-api binary.
RUN cargo build --release --locked --bin sdl-api


### Stage 2: Runtime image
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      ffmpeg \
      xvfb \
      fonts-liberation \
      libasound2 \
      libatk-bridge2.0-0 \
      libgbm1 \
      libgtk-3-0 \
      libnss3 \
      libx11-xcb1 \
      libxcomposite1 \
      libxdamage1 \
      libxrandr2 \
      libxss1 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/sdl/target/release/sdl-api /usr/local/bin/sdl-api

EXPOSE 8080
ENV SDL_API_BIND=0.0.0.0:8080

ENTRYPOINT ["xvfb-run", "-a", "-s", "-screen 0 1920x1080x24", "/usr/local/bin/sdl-api"]
