FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ cmake make && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt ./
COPY include include
COPY src src
COPY tools tools
RUN cmake -S . -B build -DLOB_BUILD_TESTS=OFF && cmake --build build -j --target engine loadgen feed_client

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/engine /src/build/loadgen /src/build/feed_client /usr/local/bin/
EXPOSE 9000 9001
ENTRYPOINT ["engine"]
CMD ["--port", "9000", "--feed-port", "9001"]
