ARG BASE
FROM ${BASE}

# Basic Setting
ARG VERSION
ENV VERSION=${VERSION}

# Copy files and install dependencies
RUN mkdir -p /opt/pydtk
COPY ./pyproject.toml ./poetry.loc[k] /opt/pydtk/
WORKDIR /opt/pydtk
RUN poetry install --no-dev --no-root \
  -E ros \
  -E pointcloud \
  -E zstd \
  || ( \
    poetry update --no-dev \
    && poetry install --no-dev --no-root \
    -E ros \
    -E pointcloud \
    -E zstd \
  )

# Install ROS-related dependencies
RUN poetry run pip install -q --no-cache-dir git+https://github.com/eric-wieser/ros_numpy.git pypcd

# Copy remaining files
COPY ./README.md /opt/pydtk/README.md
COPY ./LICENSE /opt/pydtk/LICENSE
COPY ./pydtk /opt/pydtk/pydtk
COPY ./docker-entrypoint.sh /opt/pydtk/docker-entrypoint.sh

# Installation for CLI commands
RUN poetry install -E ros -E pointcloud -E zstd --no-dev

# Default CMD
ENTRYPOINT ["/opt/pydtk/docker-entrypoint.sh"]
CMD ["/bin/bash"]
