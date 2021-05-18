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

