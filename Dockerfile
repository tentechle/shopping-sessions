# Shopping Sessions: native C++ pipeline, one static binary in an empty image.
FROM gcc:13-bookworm AS build
COPY src/main.cpp /src/main.cpp
RUN g++ -O3 -march=x86-64-v3 -mtune=cascadelake -std=c++17 -static -pthread \
        -o /pipeline /src/main.cpp && strip /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
USER 1000:1000
ENTRYPOINT ["/pipeline"]
