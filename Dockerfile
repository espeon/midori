# first stage, build the application
FROM rust:1.58.1-alpine as builder

RUN apk add --no-cache git

WORKDIR /app

COPY . .

RUN cargo build --release --target=x86_64-unknown-linux-musl

# second stage, final image
FROM alpine:latest as final

RUN apk add --no-cache \
    ffmpeg \
    ca-certificates \
    wget \
    unzip

RUN wget https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-641.x86_64-unknown-linux.zip \
    -O /tmp/bento4.zip \
    && unzip /tmp/bento4.zip -d /usr/local/bento4 \
    && ln -s /usr/local/bento4/lib/libbento4.so /usr/lib/libbento4.so

# put in PATH


WORKDIR /app

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/seiyuu_radio_subs_dl .

ENV PATH="$PATH:/usr/bin"

CMD ["./seiyuu_radio_subs_dl"]

