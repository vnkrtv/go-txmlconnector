# Build context: standalone go-txmlconnector repository root.
FROM golang:1.24.11-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
COPY proto ./proto
ARG VERSION=development
RUN CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/txmlconnector.exe ./cmd/txmlconnector

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends wine wine64 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 connector \
    && mkdir -p /var/lib/txml/logs \
    && chown -R connector:connector /var/lib/txml
WORKDIR /opt/txml
COPY --from=build /out/txmlconnector.exe ./txmlconnector.exe
ARG DLL_SOURCE=txmlconnector64-6.43.2.24.0.dll
COPY ${DLL_SOURCE} ./txmlconnector.dll
USER connector
ENV WINEARCH=win64 WINEPREFIX=/var/lib/txml/wine WINEDEBUG=-all
EXPOSE 50051 9090
STOPSIGNAL SIGINT
HEALTHCHECK --interval=15s --timeout=3s --start-period=60s \
    CMD curl --fail --silent http://127.0.0.1:9090/readyz >/dev/null || exit 1
ENTRYPOINT ["wine", "/opt/txml/txmlconnector.exe"]
CMD ["-dll", "Z:\\opt\\txml\\txmlconnector.dll", "-native-log-dir", "Z:\\var\\lib\\txml\\logs", "-grpc-address", "0.0.0.0:50051", "-http-address", "0.0.0.0:9090"]

FROM build AS test-build
RUN CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go test -c \
    -o /out/native.test.exe ./internal/native

FROM runtime AS smoke
COPY --from=test-build /out/native.test.exe /opt/txml/native.test.exe
ENV TXML_SMOKE_DLL=Z:\\opt\\txml\\txmlconnector.dll
HEALTHCHECK NONE
ENTRYPOINT ["wine", "/opt/txml/native.test.exe"]
CMD ["-test.run", "^TestDLLSmoke$", "-test.v", "-test.timeout", "45s"]

FROM runtime AS final
