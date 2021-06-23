FROM ubuntu:18.04

# Basic Setting
ENV LANG="en_US.UTF-8"
ARG VERSION
ENV VERSION=${VERSION}

# Install build tools
RUN apt update \
  && apt install -y --no-install-recommends python3.7 python3-pip git curl libgl1-mesa-glx libglib2.0-0 \
  && apt -y clean \
  && rm -rf /var/lib/apt/lists/* \
  && rm /usr/bin/python3 \
  && ln -s /usr/bin/python3.7 /usr/bin/python3 \
  && ln -s /usr/bin/python3 /usr/bin/python \
  && python3 -m pip install --no-cache-dir --upgrade pip \
  && python3 -m pip install --no-cache-dir setuptools \
  && curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 - \
  && ln -s /root/.poetry/bin/poetry /usr/bin/poetry \
  && poetry config virtualenvs.in-project true
