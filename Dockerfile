ARG UBUNTU_BASE=ubuntu:22.04@sha256:0e0a0fc6d18feda9db1590da249ac93e8d5abfea8f4c3c0c849ce512b5ef8982

FROM ${UBUNTU_BASE} AS build

ARG DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=UTC

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        cmake \
        g++ \
        make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt ./
COPY include/ include/
COPY src/ src/
COPY tests/ tests/

RUN cmake -S /src -B /build \
        -G "Unix Makefiles" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CXX_COMPILER=/usr/bin/g++ \
    && cmake --build /build --parallel 2 \
    && ctest --test-dir /build --output-on-failure \
    && cmake --install /build --prefix /opt/graph_nomemory

FROM ${UBUNTU_BASE} AS runtime

ARG DEBIAN_FRONTEND=noninteractive
ARG VCS_REF=unknown
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=UTC

LABEL org.opencontainers.image.title="Graph NoMemory" \
      org.opencontainers.image.description="Out-of-core PageRank for one Linux node" \
      org.opencontainers.image.source="https://github.com/Nikkfh5/graph_nomemory" \
      org.opencontainers.image.revision="${VCS_REF}"

COPY --from=build /opt/graph_nomemory/ /usr/local/

WORKDIR /data
CMD ["main", "--help"]
