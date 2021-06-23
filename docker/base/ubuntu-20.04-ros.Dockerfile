FROM ros:noetic

# Basic Setting
ARG VERSION
ENV VERSION=${VERSION}

# Install build tools
RUN apt update \
  && apt install -y --no-install-recommends git curl libgl1-mesa-glx \
  && apt -y clean \
  && rm -rf /var/lib/apt/lists/* \
  && ln -s /usr/bin/python3 /usr/bin/python \
  && curl https://bootstrap.pypa.io/get-pip.py | python3 \
  && python3 -m pip install --no-cache-dir --upgrade pip \
  && python3 -m pip install --no-cache-dir setuptools \
  && curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 - \
  && ln -s /root/.poetry/bin/poetry /usr/bin/poetry \
  && poetry config virtualenvs.in-project true
