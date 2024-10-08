# Copyright 2016 The Kubernetes Authors All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# network performance tests in containers
#
# Run as the Orchestrator: Arguments: --mode=orchestrator
#
# Run as a Worker (first get orchestrator virtual IP address):
# kubectl get svc --format "{{ .NetworkSettings.IPAddress }}" netperf-orch
#
# Args: --mode=worker --host=<service cluster ip> --port=5202
#
FROM golang:bullseye AS builder
WORKDIR /workspace

COPY . .

RUN go build -o nptests

FROM debian:bullseye
ENV LD_LIBRARY_PATH=/usr/local/lib

LABEL org.opencontainers.image.description "Network performance tests in k8s engine"

# install binary and remove cache
RUN apt-get update \
    && apt-get install -y  curl wget net-tools gcc make libsctp-dev git autotools-dev automake \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /tmp

RUN git clone https://github.com/linux-rdma/qperf.git \
    && cd qperf \
    && ./cleanup \
    && ./autogen.sh \
    && ./configure --prefix=/usr/local --bindir /usr/local/bin \
    && make && make install

# Download and build iperf3 from sources
RUN curl -LO https://downloads.es.net/pub/iperf/iperf-3.1.tar.gz && tar zxf iperf-3.1.tar.gz
RUN cd iperf-3.1 && ./configure --prefix=/usr/local --bindir /usr/local/bin && make && make install

# Download and build netperf from sources
RUN curl -LO https://github.com/HewlettPackard/netperf/archive/netperf-2.7.0.tar.gz && tar -xzf netperf-2.7.0.tar.gz && mv netperf-netperf-2.7.0/ netperf-2.7.0
RUN cd netperf-2.7.0 && ./configure --prefix=/usr/local --bindir /usr/local/bin && make CFLAGS=-fcommon && make install

# Validate the installation of qperf, iperf3 and netperf
RUN usr/local/bin/qperf --help
RUN usr/local/bin/iperf3 -v
RUN usr/local/bin/netperf -V
RUN usr/local/bin/netserver -V

COPY --from=builder /workspace/nptests /usr/bin/

ENTRYPOINT ["nptests"]
