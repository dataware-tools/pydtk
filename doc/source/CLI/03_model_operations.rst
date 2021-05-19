Model Operations
=========

Model related operations


List available models
*********

.. code-block:: bash

    (.venv)$ pydtk model list
    Available models with priorities:
    {1: [<class 'pydtk.models.csv.GenericCsvModel'>,
         <class 'pydtk.models.movie.GenericMovieModel'>,
         <class 'pydtk.models.json_model.GenericJsonModel'>],
     2: [<class 'pydtk.models.csv.CameraTimestampCsvModel'>],
     3: [<class 'pydtk.models.csv.AnnotationCsvModel'>,
         <class 'pydtk.models.csv.ForecastCsvModel'>]}



Test if a file can be read using PyDTK
***********

.. code-block:: bash

    (.venv)$ pydtk model is_available test/records/json_model_test/json_test.json
    True



Generate a template of metadata
***********

.. code-block:: bash

    (.venv)$ pydtk model generate template
    {
        "record_id": null,
        "description": null,
        "path": null,
        "type": null,
        "contents": null,
        "tags": null
    }



Generate metadata from a file
***********

Generate metadata from a file

.. code-block:: bash

    (.venv)$ pydtk model generate metadata --from-file test/records/csv_model_test/data/test.csv
    {
        "record_id": null,
        "description": null,
        "path": "/path/to/current/dir/test/records/csv_model_test/data/test.csv",
        "type": null,
        "contents": {
            "content": {
                "columns": [
                    "1489728491000",
                    "24",
                    "180971",
                    "1"
                ],
                "tags": [
                    "csv"
                ]
            }
        }
    }


Generate metadata from a file by specifying record_id

.. code-block:: bash

    (.venv)$ pydtk model generate metadata --from-file test/records/csv_model_test/data/test.csv --record_id abc
    {
        "record_id": "abc",
        "description": null,
        "path": "/path/to/current/dir/test/records/csv_model_test/data/test.csv",
        "type": null,
        "contents": {
            "content": {
                "columns": [
                    "1489728491000",
                    "24",
                    "180971",
                    "1"
                ],
                "tags": [
                    "csv"
                ]
            }
        }
    }

Generate metadata from a file using a template

.. code-block:: bash

    (.venv)$ pydtk model generate metadata --from-file test/records/csv_model_test/data/test.csv --template template.json --record_id abc
    {
        "record_id": "abc",
        "description": null,
        "path": "/path/to/current/dir/test/records/csv_model_test/data/test.csv",
        "type": null,
        "contents": {
            "content": {
                "columns": [
                    "1489728491000",
                    "24",
                    "180971",
                    "1"
                ],
                "tags": [
                    "csv"
                ]
            }
        },
        "tags": null,
        "end_timestamp": null,
        "content_type": null,
        "data_type": null,
        "start_timestamp": null,
        "database_id": null,
        "key-dict": null,
        "key-float": null,
        "key-int": null,
        "key-str": null
    }

