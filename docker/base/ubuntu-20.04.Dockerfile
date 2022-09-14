FROM ubuntu:20.04

# Basic Setting
ENV LANG="en_US.UTF-8"
ARG VERSION
ENV VERSION=${VERSION}

# Install build tools
ENV PATH /root/.local/bin:${PATH}
RUN apt update \
  && apt install -y --no-install-recommends python3 python3-pip git curl libgl1-mesa-glx libglib2.0-0 jq \
  && apt -y clean \
  && rm -rf /var/lib/apt/lists/* \
  && ln -s /usr/bin/python3 /usr/bin/python \
  && python3 -m pip install --no-cache-dir --upgrade pip \
  && python3 -m pip install --no-cache-dir setuptools \
  && curl -sSL https://install.python-poetry.org | python3 - \
  && ln -s /root/.poetry/bin/poetry /usr/bin/poetry \
  && poetry config virtualenvs.in-project true
