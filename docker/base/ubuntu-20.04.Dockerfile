FROM ubuntu:20.04

# Basic Setting
ENV LANG="en_US.UTF-8"
ARG VERSION
ENV VERSION=${VERSION}

# Install build tools
RUN apt update \
  && apt install -y --no-install-recommends python3 python3-pip git curl \
  && apt -y clean \
  && rm -rf /var/lib/apt/lists/* \
  && ln -s /usr/bin/python3 /usr/bin/python \
  && python3 -m pip install --no-cache-dir --upgrade pip \
  && python3 -m pip install --no-cache-dir setuptools \
  && curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 - \
  && ln -s /root/.poetry/bin/poetry /usr/bin/poetry \
  && poetry config virtualenvs.in-project true
