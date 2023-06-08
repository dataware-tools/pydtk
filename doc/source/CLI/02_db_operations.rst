DB Operations
=========

You can list, get, add and delete resources on DB as follows.


List databases
*********

List all databases

.. code-block:: bash

    (.venv)$ pydtk db list databases
          Database ID
    0     default


List all databases as a parsable string

.. code-block:: bash

    (.venv)$ pydtk db list databases --parsable
    [{'_creation_time': 1621333648.499061,
      '_id': '90e679b0b7c311eb8672acde48001122',
      '_uuid': 'c21f969b5f03d33d43e04f8f136e7682',
      'database_id': 'default',
      'df_name': 'db_0ffc6dbe_meta'}]




List records
********

List all records in database `default`

.. code-block:: bash

    (.venv)$ pydtk db list records
                              Record ID   Description                                          File path                                           Contents Tags
    0                            sample   Description                   /records/sample/data/records.bag  {'/points_concat_downsampled': {'msg_type': 's...  NaN
    1                    csv_model_test   Description              /records/csv_model_test/data/test.csv  {'camera/front-center': {'tags': ['camera', 'f...  NaN
    2                              test     json file            /records/json_model_test/json_test.json             {'test': {'tags': ['test1', 'test2']}}  NaN
    3                              test      Forecast     /records/forecast_model_test/forecast_test.csv         {'forecast': {'tags': ['test1', 'test2']}}  NaN
    4  016_00000000030000000015_1095_01  Description.  /records/annotation_model_test/annotation_test...  {'risk_annotation': {'tags': ['risk_score', 's...  NaN


List all records in database `example`

.. code-block:: bash

    (.venv)$ pydtk db list records --database_id example
    Empty DataFrame
    Columns: [Record ID, Description, File path, Contents, Tags]
    Index: []


List the first 3 records in database `default`

.. code-block:: bash

    (.venv)$ pydtk db list records --offset 0 --limit 3
                              Record ID   Description                                          File path                                           Contents Tags
    0                            sample   Description                   /records/sample/data/records.bag  {'/points_concat_downsampled': {'msg_type': 's...  NaN
    1                    csv_model_test   Description              /records/csv_model_test/data/test.csv  {'camera/front-center': {'tags': ['camera', 'f...  NaN
    2                              test     json file            /records/json_model_test/json_test.json             {'test': {'tags': ['test1', 'test2']}}  NaN


Search records using PQL (Python Query Language)

.. code-block:: bash

    (.venv)$ pydtk db list records --pql 'record_id == regex(".*test")'
            Record ID  Description                                       File path                                           Contents Tags
    0  csv_model_test  Description           /records/csv_model_test/data/test.csv  {'camera/front-center': {'tags': ['camera', 'f...  NaN
    1            test    json file         /records/json_model_test/json_test.json             {'test': {'tags': ['test1', 'test2']}}  NaN
    2            test     Forecast  /records/forecast_model_test/forecast_test.csv         {'forecast': {'tags': ['test1', 'test2']}}  NaN


Display the result as a JSON string

.. code-block:: bash

    (.venv)$ pydtk db list records --pql 'record_id == regex(".*test")' --parsable
    [{'_creation_time': 1621334935.451183,
      '_id': 'a449171cb7c611eba47dacde48001122',
      '_uuid': '999e172bd18e46661ce10898122025cb',
      'content_type': 'text/csv',
      'contents': {'camera/front-center': {'tags': ['camera',
                                                    'front',
                                                    'center',
                                                    'timestamps']}},
      'data_type': 'raw_data',
      'description': 'Description',
      'end_timestamp': 1489728570.957,
      'path': '/records/csv_model_test/data/test.csv',
      'record_id': 'csv_model_test',
      'start_timestamp': 1489728491.0},
     {'_creation_time': 1621334935.45182,
      '_id': 'a4494160b7c611eba47dacde48001122',
      '_uuid': '1a2e2cb364f2d4f43d133719c11d1867',
      'content_type': 'application/json',
      'contents': {'test': {'tags': ['test1', 'test2']}},
      'data_type': 'test',
      'database_id': 'json datbase',
      'description': 'json file',
      'path': '/records/json_model_test/json_test.json',
      'record_id': 'test'},
     {'_creation_time': 1621334935.452434,
      '_id': 'a4496582b7c611eba47dacde48001122',
      '_uuid': 'be7a0ce377de8a4f164dbd019cacb7a2',
      'content_type': 'text/csv',
      'contents': {'forecast': {'tags': ['test1', 'test2']}},
      'data_type': 'forecast',
      'description': 'Forecast',
      'path': '/records/forecast_model_test/forecast_test.csv',
      'record_id': 'test'}]


List files
********

List files

.. code-block:: bash

    (.venv)$ pydtk db list files
                              Record ID   Description                                          File path                                           Contents Tags
    0                            sample   Description                   /records/sample/data/records.bag  {'/points_concat_downsampled': {'msg_type': 's...  NaN
    1                    csv_model_test   Description              /records/csv_model_test/data/test.csv  {'camera/front-center': {'tags': ['camera', 'f...  NaN
    2                              test     json file            /records/json_model_test/json_test.json             {'test': {'tags': ['test1', 'test2']}}  NaN
    3                              test      Forecast     /records/forecast_model_test/forecast_test.csv         {'forecast': {'tags': ['test1', 'test2']}}  NaN
    4  016_00000000030000000015_1095_01  Description.  /records/annotation_model_test/annotation_test...  {'risk_annotation': {'tags': ['risk_score', 's...  NaN


List contents
********

List contents

.. code-block:: bash

    (.venv)$ pydtk db list contents
                              Record ID   Description                                          File path                                           Contents Tags
    0                            sample   Description                   /records/sample/data/records.bag  {'/points_concat_downsampled': {'msg_type': 's...  NaN
    1                    csv_model_test   Description              /records/csv_model_test/data/test.csv  {'camera/front-center': {'tags': ['camera', 'f...  NaN
    2                              test     json file            /records/json_model_test/json_test.json             {'test': {'tags': ['test1', 'test2']}}  NaN
    3                              test      Forecast     /records/forecast_model_test/forecast_test.csv         {'forecast': {'tags': ['test1', 'test2']}}  NaN
    4  016_00000000030000000015_1095_01  Description.  /records/annotation_model_test/annotation_test...  {'risk_annotation': {'tags': ['risk_score', 's...  NaN


Get resources
******

Get a specific resource

.. code-block:: bash

    (.venv)$ pydtk db get record --database_id default --record_id sample --content '/points_concat_downsampled'
       Description Record ID                         File path                                           Contents Tags  end_timestamp        content_type data_type  start_timestamp database_id key-dict  key-float  key-int key-str
    0  Description    sample  /records/sample/data/records.bag  {'/points_concat_downsampled': {'msg_type': 's...  NaN   1.550126e+09  application/rosbag  raw_data     1.550126e+09         NaN      NaN        NaN      NaN     NaN


Add resources
*******

Add metadata to DB by specifying a JSON file containing metadata

.. code-block:: bash

    (.venv)$ pydtk db add file --database_id default metadata.json


By specifying a directory containing JSON files

.. code-block:: bash

    (.venv)$ pydtk db add file --database_id default /path/to/data_dir

You can also use heredoc

.. code-block:: bash

    (.venv)$ pydtk db add file --database_id default <<EOF
    {
        "description": "Description",
        "record_id": "rosbag_model_test",
        "type": "raw_data",
        "path": "/opt/pydtk/test/records/rosbag_model_test/data/records.bag",
        "start_timestamp": 1517463303.0,
        "end_timestamp": 1517463303.95,
        "content-type": "application/rosbag",
        "contents": {
            "/vehicle/acceleration": {
                "msg_type": "geometry_msgs/AccelStamped",
                "msg_md5sum": "d8a98a5d81351b6eb0578c78557e7659",
                "count": 10,
                "frequency": 10.000009536752259,
                "tags": [
                    "vehicle",
                    "acceleration"
                ]
            }
        }
    }
    EOF

That means that you can pipe commands

.. code-block:: bash

    (.venv)$ cat metadata.json | pydtk db add file --database_id default


Delete resources
********

Delete metadata by specifying record_id

.. code-block:: bash

    (.venv)$ pydtk db delete file --record_id test
    The following data will be deleted:
      Record ID Description                                       File path                                    Contents Tags
    0      test   json file         /records/json_model_test/json_test.json      {'test': {'tags': ['test1', 'test2']}}  NaN
    1      test    Forecast  /records/forecast_model_test/forecast_test.csv  {'forecast': {'tags': ['test1', 'test2']}}  NaN
    Proceed? [y/N]: y
    Deleted.


Delete metadata using heredoc

.. code-block:: bash

    (.venv)$ pydtk db delete file <<EOF
    {
        "description": "Description",
        "record_id": "rosbag_model_test",
        "type": "raw_data",
        "path": "/opt/pydtk/test/records/rosbag_model_test/data/records.bag",
        "start_timestamp": 1517463303.0,
        "end_timestamp": 1517463303.95,
        "content-type": "application/rosbag",
        "contents": {
            "/vehicle/acceleration": {
                "msg_type": "geometry_msgs/AccelStamped",
                "msg_md5sum": "d8a98a5d81351b6eb0578c78557e7659",
                "count": 10,
                "frequency": 10.000009536752259,
                "tags": [
                    "vehicle",
                    "acceleration"
                ]
            }
        }
    }
    EOF

That means that you can pipe commands

.. code-block:: bash

    (.venv)$ cat metadata.json | pydtk db delete file
