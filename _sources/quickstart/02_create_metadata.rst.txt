Register an existing dataset
============================
You can register the metadata of an existing dataset to DB with the following command:

.. code-block:: bash

    (.venv)$ create_meta_db <database ID> <target dir> --output_db_host <output file>

where `<target dir>` and `<output file>` are the directory containing metadata json files
and a sqlite database file with extension `.db` respectively.
You can set any value for `<database ID>`, which will be used for determining table name in DB.
