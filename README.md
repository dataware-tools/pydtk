# Python Dataware Toolkit

A Python toolkit for managing, retrieving, and processing data.

## Install
You can install the toolkit with:
```bash
$ pip install git+https://github.com/dataware-tools/pydtk.git

```

## Usage

### Create database for metadata
Create database of metadata using meta_db.py
```bash
$ create_meta_db <database ID> <target dir> --output_db_host <output file>

```
where `<target dir>` and `<output file>` are the directory containing metadata json files
and a sqlite database file with extension `.db` respectively.  
You can set any value for `<database ID>`, which will be used for determining table name in DB.


### Search database

1. Load DBHandler for metadata
```python
from pydtk.db import V3DBHandler as DBHandler

# Initialize handler (This will read all the metadata from DB on initialization)
handler = DBHandler(
    db_class='meta',
    db_engine='sqlite',
    db_host='/data_pool_1/small_DrivingBehaviorDatabase/metadata.db',
    base_dir_path='/data_pool_1/small_DrivingBehaviorDatabase'
)

# Initialize handler without reading all the metadata from DB
handler = DBHandler(
    db_class='meta',
    db_engine='sqlite',
    db_host='/data_pool_1/small_DrivingBehaviorDatabase/metadata.db',
    base_dir_path='/data_pool_1/small_DrivingBehaviorDatabase',
    read_on_init=False
)

```

2. Read metadata from db with data selection.
```python
# Select by timestamp
handler.read(where='start_timestamp > 1420000000 and end_timestamp < 1500000000')
records = handler.get_record_id_df().to_dict('records')
print(records)

# Select by tags
handler.read(where='tags like "%camera%" or tags like "%lidar%"')
records = handler.get_record_id_df().to_dict('records')
print(records)

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


### Search database (deprecated)

1. Load indexed database file.
```python
from pydtk import frontend

# Load pickle file
pkl = "/data_pool_1/DrivingBehaviorDatabase/pydtk.pkl"
front = frontend.LoadPKL(pkl)
```
2. Get record ID list.
```python
record_id_list = front.get_record_id_info()
print(record_id_list[0])

# {'record_id': '016_00000000030000000012', 'duration': 546.9500000476837, 'start_timestamp': 1484290020.0, 'end_timestamp': 1484290566.95, 'tags': ['camera', 'front', ..., 'movement']}
```
3. Search DataFrame by tags.

```python
from pydtk.utils import utils

content_df = front.content_df
tags = ["image", "front"]
front_image_df = utils.tag_filter(tags, content_df)
print(front_image_df)

#                       record_id                                               path  ... msg_type                             tag
# 0      016_00000000030000000012  /data_pool_1/DrivingBehaviorDatabase/records/0...  ...     None  [camera, front, center, image]
# 2      016_00000000030000000012  /data_pool_1/DrivingBehaviorDatabase/records/0...  ...     None    [camera, front, left, image]

# ...                         ...                                                ...  ...      ...                             ...
# 16644  W08_17000000020000001288  /data_pool_1/DrivingBehaviorDatabase/records/W...  ...     None    [camera, front, left, image]
# 16646  W08_17000000020000001288  /data_pool_1/DrivingBehaviorDatabase/records/W...  ...     None   [camera, front, right, image]
```

### Read data

```python

from pydtk.io import BaseFileReader
from pydtk.preprocesses import passthrough

reader = BaseFileReader()

# add preprocess
prepro = passthrough.AddBias(5.0)
reader.add_preprocess(prepro)

# set readfile option
file = '/data_pool_1/small_DrivingBehaviorDatabase/records/016_00000000030000000215/data/records.bag'
content = '/vehicle/analog/speed_pulse'

# read data
timestamps, data, columns = reader.read(path=file, contents=content)

```


## Setup
To setup the toolkit, poetry has to be installed on your environment.  

The installation of this toolkit can be done with the following command.
```bash
$ poetry install

```

