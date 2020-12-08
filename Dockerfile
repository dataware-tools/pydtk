FROM ros:melodic

# Basic Setting
ENV LANG="en_US.UTF-8"

# Install pip
RUN apt update \
  && apt install -y --no-install-recommends python3-pip python3-venv pandoc \
  && apt install -y --no-install-recommends default-mysql-client postgresql-client \
  && apt -y clean \
  && rm -rf /var/lib/apt/lists/* \
  && python3 -m pip install --upgrade pip \
  && python3 -m pip install setuptools \
  && python3 -m pip install pyyaml gnupg rospkg pycryptodome \
  && python3 -m pip install git+https://github.com/eric-wieser/ros_numpy.git@0.0.3 \
  && python3 -m pip install poetry \
  && poetry config virtualenvs.create false

# Install dev tools and database tools
RUN apt-get update \
  && apt-get -y install vim tmux parallel python3-psycopg2

# Copy files and install dependencies
RUN mkdir -p /opt/pydtk
COPY ./pyproject.toml ./poetry.loc[k] /opt/pydtk/
WORKDIR /opt/pydtk
RUN poetry install -E ros -E cassandra -E mysql -E postgresql || poetry update
ENV PYTHONPATH /opt/pydtk:${PYTHONPATH}

# Copy remaining files
COPY . /opt/pydtk

# Default CMD
CMD ["/bin/bash"]
