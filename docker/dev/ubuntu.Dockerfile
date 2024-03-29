ARG BASE
FROM ${BASE}

# Basic Setting
ARG VERSION
ENV VERSION=${VERSION}

# Install dev tools and database tools
RUN apt-get update \
  && apt-get -y install build-essential vim tmux parallel pandoc \
  && apt -y clean \
  && rm -rf /var/lib/apt/lists/*

# Copy files and install dependencies
RUN mkdir -p /opt/pydtk
COPY ./pyproject.toml ./poetry.loc[k] /opt/pydtk/
WORKDIR /opt/pydtk
RUN poetry install -E zstd \
  || ( \
    poetry update \
    && poetry install -E zstd \
  )

# Copy remaining files
COPY . /opt/pydtk

# Installation for CLI commands
RUN poetry install -E zstd

# Default CMD
ENTRYPOINT ["/opt/pydtk/docker-entrypoint.sh"]
CMD ["/bin/bash"]
