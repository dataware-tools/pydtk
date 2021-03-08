# Python Dataware Toolkit

A Python toolkit for managing, retrieving, and processing data.

## Installation
You can install the toolkit with:
```bash
$ pip3 install git+https://github.com/dataware-tools/pydtk.git

```

If you want to install the toolkit with extra feature (e.g. support for MongoDB and ROS), 
you can install extra dependencies as follows:
```bash
$ pip3 install git+https://github.com/dataware-tools/pydtk.git#egg=pydtk[mongodb,ros]

```


## Usage

By using Pydtk, you can load a variety of types of data with a unified interface as shown below.

1. Load DBHandler for retrieving metadata
```python
from pydtk.db import V4DBHandler as DBHandler

# Initialize handler (This will read all the metadata from DB on initialization)
handler = DBHandler(
    db_class='meta',
    db_host='./examples/example_db',
    base_dir_path='./test'
)

```

2. Read metadata from db with data selection.
```python
# Select by timestamp
handler.read(pql='start_timestamp > 1420000000 and end_timestamp < 1500000000')
print(handler.data)

# Select by record-id
handler.read(pql='record_id == regex("B05.*")')
print(handler.data)

```

3. Load data from files based on metadata.
```python
from pydtk.io import BaseFileReader, NoModelMatchedError

reader = BaseFileReader()

try:
    for sample in handler:
        print('loading content "{0}" from file "{1}"'.format(sample['contents'], sample['path']))
        try:
            timestamps, data, columns = reader.read(sample)
            assert print(data)
        except NoModelMatchedError as e:
            print(str(e))
            continue
except EOFError:
    pass
```


## Documentation
For more information about this toolkit, please refer the [document](https://dataware-tools.github.io/pydtk/).


## Setup for contribution
To improve this toolkit, firstly clone this repository and then 
run the following command to prepare the environment. 

```bash
$ poetry install

```

Make sure that [poetry](https://python-poetry.org/) is installed before executing the command.

If you want to install the toolkit with extra feature (e.g. support for MongoDB), 
please specify it with `-E` option.  
Example (installation with `mongodb` and `ros` extras):
```bash
$ poetry install -E mongodb -E ros

```
