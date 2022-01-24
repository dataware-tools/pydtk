Search.setIndex({docnames:["CLI/01_overview","CLI/02_db_operations","CLI/03_model_operations","CLI/04_io_operations","examples/01_grab_metadata","examples/02_grab_data_based_on_metadata","examples/03_save_data","index","modules/modules","modules/pydtk","modules/pydtk.bin","modules/pydtk.bin.sub_commands","modules/pydtk.builder","modules/pydtk.builder.dbdb","modules/pydtk.db","modules/pydtk.db.v1","modules/pydtk.db.v1.handlers","modules/pydtk.db.v2","modules/pydtk.db.v2.handlers","modules/pydtk.db.v2.search_engines","modules/pydtk.db.v3","modules/pydtk.db.v3.handlers","modules/pydtk.db.v3.search_engines","modules/pydtk.db.v4","modules/pydtk.db.v4.deps","modules/pydtk.db.v4.engines","modules/pydtk.db.v4.handlers","modules/pydtk.io","modules/pydtk.models","modules/pydtk.models.autoware","modules/pydtk.models.pointcloud","modules/pydtk.models.zstd","modules/pydtk.preprocesses","modules/pydtk.statistics","modules/pydtk.utils","quickstart/01_try_pydtk","quickstart/02_create_metadata"],envversion:{"sphinx.domains.c":2,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":3,"sphinx.domains.index":1,"sphinx.domains.javascript":2,"sphinx.domains.math":2,"sphinx.domains.python":2,"sphinx.domains.rst":2,"sphinx.domains.std":2,nbsphinx:4,sphinx:56},filenames:["CLI/01_overview.rst","CLI/02_db_operations.rst","CLI/03_model_operations.rst","CLI/04_io_operations.rst","examples/01_grab_metadata.ipynb","examples/02_grab_data_based_on_metadata.ipynb","examples/03_save_data.ipynb","index.rst","modules/modules.rst","modules/pydtk.rst","modules/pydtk.bin.rst","modules/pydtk.bin.sub_commands.rst","modules/pydtk.builder.rst","modules/pydtk.builder.dbdb.rst","modules/pydtk.db.rst","modules/pydtk.db.v1.rst","modules/pydtk.db.v1.handlers.rst","modules/pydtk.db.v2.rst","modules/pydtk.db.v2.handlers.rst","modules/pydtk.db.v2.search_engines.rst","modules/pydtk.db.v3.rst","modules/pydtk.db.v3.handlers.rst","modules/pydtk.db.v3.search_engines.rst","modules/pydtk.db.v4.rst","modules/pydtk.db.v4.deps.rst","modules/pydtk.db.v4.engines.rst","modules/pydtk.db.v4.handlers.rst","modules/pydtk.io.rst","modules/pydtk.models.rst","modules/pydtk.models.autoware.rst","modules/pydtk.models.pointcloud.rst","modules/pydtk.models.zstd.rst","modules/pydtk.preprocesses.rst","modules/pydtk.statistics.rst","modules/pydtk.utils.rst","quickstart/01_try_pydtk.rst","quickstart/02_create_metadata.rst"],objects:{"":{pydtk:[9,0,0,"-"]},"pydtk.bin":{cli:[10,0,0,"-"],make_meta:[10,0,0,"-"],sub_commands:[11,0,0,"-"]},"pydtk.bin.cli":{CLI:[10,1,1,""],script:[10,3,1,""]},"pydtk.bin.cli.CLI":{db:[10,2,1,""],io:[10,2,1,""],model:[10,2,1,""],status:[10,2,1,""],version:[10,2,1,""]},"pydtk.bin.make_meta":{get_arguments:[10,3,1,""],main:[10,3,1,""],make_meta:[10,3,1,""],make_meta_interactively:[10,3,1,""]},"pydtk.bin.sub_commands":{db:[11,0,0,"-"],io:[11,0,0,"-"],model:[11,0,0,"-"],status:[11,0,0,"-"]},"pydtk.bin.sub_commands.db":{DB:[11,1,1,""],EmptySTDINError:[11,4,1,""]},"pydtk.bin.sub_commands.db.DB":{"delete":[11,2,1,""],add:[11,2,1,""],get:[11,2,1,""],list:[11,2,1,""]},"pydtk.bin.sub_commands.io":{IO:[11,1,1,""]},"pydtk.bin.sub_commands.io.IO":{read:[11,2,1,""]},"pydtk.bin.sub_commands.model":{Model:[11,1,1,""]},"pydtk.bin.sub_commands.model.Model":{generate:[11,2,1,""],is_available:[11,2,1,""],list:[11,2,1,""]},"pydtk.bin.sub_commands.status":{STATUS:[11,1,1,""]},"pydtk.bin.sub_commands.status.STATUS":{access:[11,2,1,""],env:[11,2,1,""],environment:[11,2,1,""]},"pydtk.builder":{concat_df:[12,0,0,"-"],dbdb:[13,0,0,"-"],df_list:[12,0,0,"-"],df_path:[12,0,0,"-"],meta_db:[12,0,0,"-"],statistic_db:[12,0,0,"-"]},"pydtk.builder.concat_df":{concat_df:[12,3,1,""]},"pydtk.builder.df_list":{build_df_list:[12,3,1,""],find_json:[12,3,1,""]},"pydtk.builder.df_path":{change_path_df:[12,3,1,""]},"pydtk.builder.meta_db":{main:[12,3,1,""],script:[12,3,1,""]},"pydtk.builder.statistic_db":{batch_analysis:[12,3,1,""],batch_script:[12,3,1,""],main:[12,3,1,""],script:[12,3,1,""]},"pydtk.db":{exceptions:[14,0,0,"-"],v2:[17,0,0,"-"],v3:[20,0,0,"-"],v4:[23,0,0,"-"]},"pydtk.db.exceptions":{DatabaseNotInitializedError:[14,4,1,""],InvalidDatabaseConfigError:[14,4,1,""]},"pydtk.db.v2":{handlers:[18,0,0,"-"],search_engines:[19,0,0,"-"]},"pydtk.db.v2.handlers":{BaseDBHandler:[18,1,1,""],TimeSeriesDBHandler:[18,1,1,""],meta:[18,0,0,"-"]},"pydtk.db.v2.handlers.BaseDBHandler":{add_data:[18,2,1,""],add_list_of_data:[18,2,1,""],columns:[18,2,1,""],db_defaults:[18,5,1,""],default_config:[18,2,1,""],df:[18,2,1,""],df_name:[18,2,1,""],read:[18,2,1,""],save:[18,2,1,""]},"pydtk.db.v2.handlers.TimeSeriesDBHandler":{db_defaults:[18,5,1,""],read:[18,2,1,""],save:[18,2,1,""]},"pydtk.db.v2.handlers.meta":{MetaDBHandler:[18,1,1,""]},"pydtk.db.v2.handlers.meta.MetaDBHandler":{add_list_of_data:[18,2,1,""],content_df:[18,2,1,""],file_df:[18,2,1,""],get_content_df:[18,2,1,""],get_file_df:[18,2,1,""],get_record_id_df:[18,2,1,""],record_id_df:[18,2,1,""]},"pydtk.db.v2.search_engines":{BaseDBSearchEngine:[19,1,1,""],time_series:[19,0,0,"-"]},"pydtk.db.v2.search_engines.BaseDBSearchEngine":{clear:[19,2,1,""],condition:[19,2,1,""],query:[19,2,1,""],search:[19,2,1,""],select:[19,2,1,""]},"pydtk.db.v2.search_engines.time_series":{TimeSeriesDBSearchEngine:[19,1,1,""]},"pydtk.db.v2.search_engines.time_series.TimeSeriesDBSearchEngine":{add_condition:[19,2,1,""],select:[19,2,1,""]},"pydtk.db.v3":{handlers:[21,0,0,"-"],search_engines:[22,0,0,"-"]},"pydtk.db.v3.handlers":{BaseDBHandler:[21,1,1,""],map_dtype:[21,3,1,""],meta:[21,0,0,"-"],register_handler:[21,3,1,""],register_handlers:[21,3,1,""],statistics:[21,0,0,"-"],time_series:[21,0,0,"-"]},"pydtk.db.v3.handlers.BaseDBHandler":{columns:[21,2,1,""],count_total:[21,2,1,""],df:[21,2,1,""],read:[21,2,1,""],save:[21,2,1,""]},"pydtk.db.v3.handlers.meta":{DatabaseIDDBHandler:[21,1,1,""],MetaDBHandler:[21,1,1,""]},"pydtk.db.v3.handlers.meta.MetaDBHandler":{add_list_of_data:[21,2,1,""],content_df:[21,2,1,""],file_df:[21,2,1,""],get_content_df:[21,2,1,""],get_file_df:[21,2,1,""],get_record_id_df:[21,2,1,""],record_id_df:[21,2,1,""],save:[21,2,1,""]},"pydtk.db.v3.handlers.statistics":{StatisticsCassandraDBHandler:[21,1,1,""],StatisticsDBHandler:[21,1,1,""]},"pydtk.db.v3.handlers.time_series":{TimeSeriesCassandraDBHandler:[21,1,1,""],TimeSeriesDBHandler:[21,1,1,""]},"pydtk.db.v3.handlers.time_series.TimeSeriesCassandraDBHandler":{df_name:[21,2,1,""],read:[21,2,1,""],save:[21,2,1,""]},"pydtk.db.v3.handlers.time_series.TimeSeriesDBHandler":{db_defaults:[21,5,1,""],save:[21,2,1,""]},"pydtk.db.v3.search_engines":{BaseDBSearchEngine:[22,1,1,""],register_engine:[22,3,1,""],register_engines:[22,3,1,""],time_series:[22,0,0,"-"]},"pydtk.db.v3.search_engines.BaseDBSearchEngine":{add_condition:[22,2,1,""],clear:[22,2,1,""],condition:[22,2,1,""],query:[22,2,1,""],search:[22,2,1,""],select:[22,2,1,""]},"pydtk.db.v3.search_engines.time_series":{TimeSeriesCassandraDBSearchEngine:[22,1,1,""]},"pydtk.db.v3.search_engines.time_series.TimeSeriesCassandraDBSearchEngine":{add_condition:[22,2,1,""],query:[22,2,1,""]},"pydtk.db.v4":{deps:[24,0,0,"-"],engines:[25,0,0,"-"],handlers:[26,0,0,"-"]},"pydtk.db.v4.engines":{mongodb:[25,0,0,"-"],register_engines:[25,3,1,""],tinydb:[25,0,0,"-"],tinymongo:[25,0,0,"-"]},"pydtk.db.v4.engines.mongodb":{connect:[25,3,1,""],drop_table:[25,3,1,""],exist_table:[25,3,1,""],read:[25,3,1,""],remove:[25,3,1,""],upsert:[25,3,1,""]},"pydtk.db.v4.engines.tinydb":{connect:[25,3,1,""],drop_table:[25,3,1,""],exist_table:[25,3,1,""],read:[25,3,1,""],remove:[25,3,1,""],upsert:[25,3,1,""]},"pydtk.db.v4.engines.tinymongo":{Document:[25,1,1,""],StorageProxy:[25,1,1,""],connect:[25,3,1,""],drop_table:[25,3,1,""],exist_table:[25,3,1,""],read:[25,3,1,""],remove:[25,3,1,""],upsert:[25,3,1,""]},"pydtk.db.v4.handlers":{BaseDBHandler:[26,1,1,""],ConfigDict:[26,1,1,""],meta:[26,0,0,"-"],register_handler:[26,3,1,""],register_handlers:[26,3,1,""]},"pydtk.db.v4.handlers.BaseDBHandler":{add_data:[26,2,1,""],columns:[26,2,1,""],config:[26,2,1,""],count_total:[26,2,1,""],data:[26,2,1,""],db_defaults:[26,5,1,""],df:[26,2,1,""],drop_table:[26,2,1,""],read:[26,2,1,""],remove_data:[26,2,1,""],save:[26,2,1,""]},"pydtk.db.v4.handlers.meta":{DatabaseIDDBHandler:[26,1,1,""],MetaDBHandler:[26,1,1,""]},"pydtk.db.v4.handlers.meta.DatabaseIDDBHandler":{remove_data:[26,2,1,""]},"pydtk.db.v4.handlers.meta.MetaDBHandler":{add_data:[26,2,1,""],add_file:[26,2,1,""],add_record:[26,2,1,""],data:[26,2,1,""],df:[26,2,1,""],read:[26,2,1,""],remove_data:[26,2,1,""],remove_file:[26,2,1,""],remove_record:[26,2,1,""],save:[26,2,1,""]},"pydtk.frontend":{LoadDB:[9,1,1,""],LoadPKL:[9,1,1,""]},"pydtk.frontend.LoadDB":{get_record_id_info:[9,2,1,""]},"pydtk.frontend.LoadPKL":{get_record_id_info:[9,2,1,""]},"pydtk.io":{errors:[27,0,0,"-"],reader:[27,0,0,"-"],writer:[27,0,0,"-"]},"pydtk.io.errors":{NoModelMatchedError:[27,4,1,""]},"pydtk.io.reader":{BaseFileReader:[27,1,1,""]},"pydtk.io.reader.BaseFileReader":{add_preprocess:[27,2,1,""],model:[27,2,1,""],read:[27,2,1,""]},"pydtk.io.writer":{BaseFileWriter:[27,1,1,""]},"pydtk.io.writer.BaseFileWriter":{model:[27,2,1,""],write:[27,2,1,""]},"pydtk.models":{BaseModel:[28,1,1,""],MetaDataModel:[28,1,1,""],UnsupportedFileError:[28,4,1,""],csv:[28,0,0,"-"],json_model:[28,0,0,"-"],register_model:[28,3,1,""],register_models:[28,3,1,""]},"pydtk.models.BaseModel":{columns:[28,2,1,""],configure:[28,2,1,""],data:[28,2,1,""],downsample_timestamps:[28,2,1,""],generate_contents_meta:[28,2,1,""],generate_timestamp_meta:[28,2,1,""],is_loadable:[28,2,1,""],load:[28,2,1,""],metadata:[28,2,1,""],save:[28,2,1,""],timestamps:[28,2,1,""],to_dict:[28,2,1,""],to_ndarray:[28,2,1,""],to_str:[28,2,1,""]},"pydtk.models.MetaDataModel":{data:[28,2,1,""],is_loadable:[28,2,1,""],load:[28,2,1,""],save:[28,2,1,""],to_dict:[28,2,1,""],to_str:[28,2,1,""]},"pydtk.models.csv":{AnnotationCsvModel:[28,1,1,""],CameraTimestampCsvModel:[28,1,1,""],ForecastCsvModel:[28,1,1,""],GenericCsvModel:[28,1,1,""]},"pydtk.models.csv.AnnotationCsvModel":{timestamps:[28,2,1,""],to_ndarray:[28,2,1,""]},"pydtk.models.csv.CameraTimestampCsvModel":{timestamps:[28,2,1,""],to_ndarray:[28,2,1,""]},"pydtk.models.csv.ForecastCsvModel":{timestamps:[28,2,1,""],to_ndarray:[28,2,1,""]},"pydtk.models.csv.GenericCsvModel":{generate_contents_meta:[28,2,1,""],generate_timestamp_meta:[28,2,1,""],timestamps:[28,2,1,""],to_ndarray:[28,2,1,""]},"pydtk.models.json_model":{GenericJsonModel:[28,1,1,""]},"pydtk.models.json_model.GenericJsonModel":{generate_contents_meta:[28,2,1,""],generate_timestamp_meta:[28,2,1,""],timestamps:[28,2,1,""],to_ndarray:[28,2,1,""]},"pydtk.preprocesses":{downsampling:[32,0,0,"-"],passthrough:[32,0,0,"-"],preprocess:[32,0,0,"-"]},"pydtk.preprocesses.downsampling":{Downsample:[32,1,1,""]},"pydtk.preprocesses.downsampling.Downsample":{processing:[32,2,1,""]},"pydtk.preprocesses.passthrough":{AddBias:[32,1,1,""],PassThrough:[32,1,1,""]},"pydtk.preprocesses.passthrough.AddBias":{processing:[32,2,1,""]},"pydtk.preprocesses.passthrough.PassThrough":{processing:[32,2,1,""]},"pydtk.preprocesses.preprocess":{BasePreprocess:[32,1,1,""]},"pydtk.preprocesses.preprocess.BasePreprocess":{processing:[32,2,1,""]},"pydtk.statistics":{BaseStatisticCalculation:[33,1,1,""],calculator:[33,0,0,"-"]},"pydtk.statistics.BaseStatisticCalculation":{calculate:[33,2,1,""],count:[33,2,1,""],max:[33,2,1,""],mean:[33,2,1,""],min:[33,2,1,""],statistic_tables:[33,2,1,""]},"pydtk.statistics.calculator":{BaseCalculator:[33,1,1,""],BoolCalculator:[33,1,1,""],FloatCalculator:[33,1,1,""],UnsupportedOperationError:[33,4,1,""]},"pydtk.statistics.calculator.BaseCalculator":{count:[33,2,1,""],divide:[33,2,1,""],max:[33,2,1,""],mean:[33,2,1,""],min:[33,2,1,""]},"pydtk.statistics.calculator.BoolCalculator":{count:[33,2,1,""],max:[33,2,1,""],mean:[33,2,1,""],min:[33,2,1,""]},"pydtk.statistics.calculator.FloatCalculator":{max:[33,2,1,""],mean:[33,2,1,""],min:[33,2,1,""]},"pydtk.utils":{can_decoder:[34,0,0,"-"],utils:[34,0,0,"-"]},"pydtk.utils.can_decoder":{BitAssign:[34,1,1,""],BitAssignInfo:[34,1,1,""],CANData:[34,1,1,""],CANDecoder:[34,1,1,""],CANDeserializer:[34,1,1,""],main:[34,3,1,""]},"pydtk.utils.can_decoder.BitAssign":{reformat:[34,2,1,""]},"pydtk.utils.can_decoder.BitAssignInfo":{bit_assigns_from_can_id:[34,2,1,""],load_from_csv:[34,2,1,""]},"pydtk.utils.can_decoder.CANDecoder":{analyze_csv:[34,2,1,""],analyze_line:[34,2,1,""],close_csv:[34,2,1,""],load_bit_assign_list:[34,2,1,""],open_csv:[34,2,1,""],unpack_data:[34,2,1,""]},"pydtk.utils.can_decoder.CANDeserializer":{deserialize:[34,2,1,""]},"pydtk.utils.utils":{deserialize_dict_1d:[34,3,1,""],dict_reg_match:[34,3,1,""],dict_to_listed_dict_1d:[34,3,1,""],dicts_to_listed_dict_2d:[34,3,1,""],dtype_string_to_dtype_object:[34,3,1,""],get_record_id_list:[34,3,1,""],listed_dict_to_dict_1d:[34,3,1,""],load_config:[34,3,1,""],search_content:[34,3,1,""],serialize_dict_1d:[34,3,1,""],smart_open:[34,3,1,""],tag_filter:[34,3,1,""],take_time_and:[34,3,1,""]},pydtk:{bin:[10,0,0,"-"],builder:[12,0,0,"-"],db:[14,0,0,"-"],frontend:[9,0,0,"-"],io:[27,0,0,"-"],models:[28,0,0,"-"],preprocesses:[32,0,0,"-"],statistics:[33,0,0,"-"],utils:[34,0,0,"-"]}},objnames:{"0":["py","module","Python module"],"1":["py","class","Python class"],"2":["py","method","Python method"],"3":["py","function","Python function"],"4":["py","exception","Python exception"],"5":["py","attribute","Python attribute"]},objtypes:{"0":"py:module","1":"py:class","2":"py:method","3":"py:function","4":"py:exception","5":"py:attribute"},terms:{"000009536752259":1,"016_00000000030000000015_1095_01":[1,4],"10222616b79211eb92da0242ac160002":6,"1158d486dd51d683ce2f1be655c3c181":4,"1484628818":4,"1484628823":4,"1489728491":[1,5,6],"1489728491000":2,"1489728570":[1,5,6],"1500000000":4,"1517463303":1,"1550125637":4,"1621304696":4,"1621333648":1,"1621334935":1,"1700000000":4,"17602878":34,"180971":2,"1a2e2cb364f2d4f43d133719c11d1867":[1,4],"2400":5,"30076":18,"3ca3cc9cb78011eb8fc30242ac110002":4,"3ca455d6b78011eb8fc30242ac110002":4,"3ca4ba76b78011eb8fc30242ac110002":4,"3ca5274ab78011eb8fc30242ac110002":4,"3cce85d6b78011eb8fc30242ac110002":4,"451183":1,"45182":1,"452434":1,"484629e":4,"489728e":6,"489729e":6,"499061":1,"517463e":6,"550126e":[1,4,6],"621305e":4,"621312e":6,"6ee6a5ceb79111eba234acde48001122":5,"7238009180df6ecbddc6bcccd530553b":6,"823646":4,"824871":4,"8263":4,"827451":4,"90e679b0b7c311eb8672acde48001122":1,"957":[1,5,6],"999e172bd18e46661ce10898122025cb":[1,6],"9ad8e31877e6befe1ad9ec1968da4c9b":6,"9d78d143650bec29f293f35142f5528c":4,"abstract":28,"case":[4,11],"class":[2,9,10,11,18,19,21,22,25,26,27,28,32,33,34],"default":[1,4,11,12,18,25],"float":[1,2,12,19,27,28,33,34],"function":[4,5,7,10,12,18,21,25,26,27,32,34],"import":[4,5,6],"int":[1,2,11,12,21,25,26,34],"long":4,"null":2,"return":[4,5,9,12,18,19,21,22,25,26,27,28,32,33,34],"static":[10,11,34],"true":[2,4,5,18,21,25,26,27,33,34],"try":[5,7],"void":27,Fps:34,IDs:25,ROS:7,That:1,The:[1,4,7,28],_config:28,_creation_tim:[1,4,6],_id:[1,4,5,6],_uuid:[1,4,6,26],a449171cb7c611eba47dacde48001122:1,a4494160b7c611eba47dacde48001122:1,a4496582b7c611eba47dacde48001122:1,abc:[2,11,26,28],abov:4,absolut:12,accel_linear_x:[19,22],acceler:[1,6,19,22],accelstamp:1,access:[0,11],actual:[4,5],adca5faea1d2012b809688628c8adcfc:[4,6],add:[7,11,18,19,21,22,26,27,32],add_condit:[19,22],add_data:[6,18,26],add_fil:26,add_list_of_data:[18,21],add_preprocess:27,add_record:26,addbia:32,adding:11,against:11,aggreg:25,all:[1,4,12],also:[1,4,5,26],amount:4,analyze_csv:34,analyze_lin:34,ani:[25,28,36],annot:[4,28],annotation_model_test:[1,4],annotation_test:[1,4],annotationcsvmodel:[2,28],anoth:4,apach:21,appli:26,applic:[1,4,6],appropri:[5,7],arg:[18,21,26,34],argument:[10,34],argumentpars:34,arrai:[5,27],as_gener:[27,28],as_ndarrai:27,assign:34,associ:5,attrdict:[18,34],automat:5,autowar:[5,6,9,28],avail:[7,11],averag:33,avoid:25,backend:21,bag:[1,4,6],base:[7,9,10,11,12,14,18,19,21,22,25,26,27,28,32,33,34,35],base_df:34,base_dir:[11,12],base_dir_path:[4,5,6,18],basecalcul:33,basedbhandl:[18,21,25,26],basedbsearchengin:[19,22],baseexcept:[14,27,28,33],basefileread:[5,27],basefilewrit:27,basemodel:28,basepreprocess:32,basestatisticcalcul:33,bash:11,batch_analysi:12,batch_script:12,be7a0ce377de8a4f164dbd019cacb7a2:[1,4],befor:4,behavior:12,being:28,between:7,bia:32,bin:[8,9],bit:34,bit_assign:34,bit_assigns_from_can_id:34,bitassign:34,bitassigninfo:34,bool:[11,12,18,21,25,26,27,28,34],boolcalcul:33,both:34,build_df_list:12,builder:[8,9],c21f969b5f03d33d43e04f8f136e7682:[1,4],calcul:[8,9,21],call:[4,5,26],camera:[1,4,5,6,28],cameratimestampcsvmodel:[2,28],can:[0,1,4,5,7,34,35,36],can_data:34,can_decod:[8,9],can_id:34,can_packet:[9,28],candata:34,candecod:34,candeseri:34,cascad:26,cassandra:21,cat:1,center:[1,5,6],chang:12,change_path_df:12,charact:34,check:[5,10,11,25,28],checker:11,choos:5,chosen:28,classmethod:[18,28],clean:[19,22],clear:[19,22],cli:[0,8,9,11],close:34,close_csv:34,collect:[25,26],collection_nam:25,column1:25,column:[1,2,5,18,21,25,26,28,33],com:[7,34],command:[1,7,10,11,36],compon:7,compos:7,concat_df:[8,9],concatin:12,condit:[19,22],config:[6,14,26,34],configdict:26,configur:[18,28],connect:25,constant:32,contain:[1,4,5,11,18,21,26,28,34,36],content:[2,4,5,6,7,8],content_df:[5,18,21,34],content_kei:28,content_typ:[1,2,4,5,6,18,21,28],convert:34,correspond:[4,5,26,27,34],cost:4,count:[1,4,25,33],count_tot:[21,26],creat:12,create_meta_db:36,csv:[1,2,4,5,6,8,9,11,34],csv_model_test:[1,2,5,6],current:[2,4,34],cursor:[11,21,25,26],custom:[25,26],d8a98a5d81351b6eb0578c78557e7659:1,data:[0,1,2,4,6,7,10,11,12,18,21,22,25,26,27,28,32,33,34,35],data_dir:1,data_in:[18,21,26],data_label:34,data_length:34,data_nam:34,data_posit:34,data_str:34,data_typ:[1,2,4,5,6,28],databas:[7,9,10,11,12,14,18,19,21,22,25,26,35,36],database_id:[1,2,4,11,12],databaseiddbhandl:[21,26],databasenotinitializederror:14,datafram:[1,4,12,18,19,21,22,26,33,34],dataframecli:[18,21,25,26],dataset:[4,7],datawar:[18,21],datbas:[1,4],date:4,datetim:34,db_0ffc6dbe_meta:[1,4],db_class:[4,5,6,21,26],db_default:[18,21,26],db_dir:12,db_engin:[18,21,26],db_handler:[4,5,19,22],db_host:[4,5,6,18,25],db_id_handl:4,db_name:[18,25],db_password:[18,25],db_usernam:[18,25],dbdb:[9,12],dbhandler:[4,5,6,25],decod:34,default_config:18,delet:[7,11],dep:[14,23],depend:7,deprec:26,describ:7,descript:[1,2,4,5,6],deseri:34,deserialize_dict_1d:34,determin:36,df_dict:33,df_list:[8,9],df_name:[1,4,18,21,26],df_path:[8,9],dic:34,dict:[1,2,4,5,11,18,21,25,26,27,28,33,34],dict_in:34,dict_reg_match:34,dict_to_listed_dict_1d:34,dictionari:26,dicts_in:34,dicts_to_listed_dict_2d:34,dir:[2,36],directori:[1,11,12,35,36],disable_count_tot:[21,25],displai:[1,11],display_nam:26,divid:[12,33],divided_data:33,divided_timestamp:33,doc_id:25,document:25,done:5,downsampl:[4,8,9,28,33],downsample_timestamp:28,downsampled_timestamp:[28,32],downsampled_valu:32,drive:12,drop:[25,26],drop_tabl:[25,26],dtype:[21,34],dtype_string_to_dtype_object:34,dummi:32,duplic:[18,21],durat:[4,18,21],dure:33,each:[4,5,12,28,34],easili:0,eba6f104b79111eb92da0242ac160002:6,eba739deb79111eb92da0242ac160002:6,eba7a392b79111eb92da0242ac160002:6,egg:7,element:[11,34],empti:[1,11],emptystdinerror:11,end:[12,27,28,34],end_tim:34,end_timestamp:[1,2,4,5,6,18,21,27,28,34],engin:[12,14,18,19,21,22,23,26],env:11,environ:11,eof:1,error:[8,9,28,33],etc:4,exampl:[1,7,11,35],example_db:[4,5],except:[5,8,9,11,27,28,33],execut:[4,19,22],exist:[4,5,7,11,25],exist_t:25,express:[4,34],extens:36,extra:7,extract:34,fail:[5,6],fals:[4,11,12,18,21,25,26,27,28,33],featur:7,figur:7,file:[4,5,7,10,11,12,26,27,28,33,34,36],file_df:[18,21,34],file_info:[9,12],filenam:34,fileutil:[8,9],filter:[4,18,21,26],find_json:12,first:1,firstli:4,floatcalcul:33,follow:[1,4,5,7,11,36],forecast:[1,4],forecast_model_test:[1,4],forecast_test:[1,4],forecastcsvmodel:[2,28],format:[4,5,25,28],fps:34,frame:[5,12],frequenc:[1,4],from:[5,7,11,18,21,25,26,28,34,35],from_fil:11,front:[1,4,5,6,28],frontend:8,gener:[7,11,21,27,28],generate_contents_meta:28,generate_timestamp_meta:28,genericcsvmodel:[2,11,28],genericjsonmodel:[2,28],genericmoviemodel:2,geomet:6,geometry_msg:1,get:[5,7,11,28,34],get_argu:10,get_content_df:[18,21],get_file_df:[18,21],get_record_id_df:[18,21],get_record_id_info:9,get_record_id_list:34,git:7,github:7,given:[5,11,27,28,34],grab:[7,35],group:[21,26],group_bi:[21,25,26],handl:[4,34],handler:[4,6,7,14,15,17,19,20,22,23,25],has:[5,12],have:5,here:5,heredoc:1,higher:28,host:[12,18,21,25,26],how:[4,5,34],howev:4,http:[7,34],huge:4,ignore_dtype_mismatch:26,imag:[8,9],in_pkl:12,includ:[33,34],index:[1,6,7],index_timestamp:33,influxdb:[18,21,25,26],info:9,inform:[4,7,9,34],initi:[4,14],input:[11,12,32,33,34],instead:4,interact:10,invaliddatabaseconfigerror:14,is_avail:[2,11],is_load:28,item:[4,18,21,25,26],iter:4,job:12,json:[1,2,3,4,11,12,26,28,36],json_model:[2,8,9],json_model_test:[1,2,3,4],json_t:4,json_test:[1,2,3,4],jupyer:35,just:5,keep:4,kei:[1,2,4,11,25,26,28],kwarg:[11,18,21,22,25,26,27,28,33,34],languag:[1,11,26],larg:4,last:11,learn:[4,5],len:[4,5],let:[4,5],lidar:4,like:[5,28],limit:[1,4,11,21,25,26],line:[4,34],list:[6,7,9,11,12,18,21,25,26,28,33,34],listed_dict_to_dict_1d:34,load:[4,5,6,27,28,34],load_bit_assign_list:34,load_config:34,load_from_csv:34,loadabl:28,loaddb:9,loader:28,loadpkl:9,localhost:18,main:[7,10,12,34],make:[10,12,33],make_meta:[8,9],make_meta_interact:10,manag:[7,10],map_dtyp:21,mapper:21,match:[27,34],max:33,maximum:33,mean:[1,19,22,33],merg:26,meta:[4,5,6,7,14,15,17,20,23],meta_db:[8,9],meta_db_base_dir:12,meta_db_engin:12,meta_db_host:12,meta_db_nam:12,meta_db_password:12,meta_db_usernam:12,metadata:[1,7,10,11,12,26,27,28,35,36],metadatamodel:[6,27,28],metadb:[18,21,26],metadbhandl:[18,21,26],method:[4,5],min:33,mind:4,minimum:33,minimum_renewal_tim:34,minimum_renewal_time_com:34,mode:[12,32,34],model:[5,6,7,8,9,10,27],model_kwarg:27,modifi:26,modul:[7,8],mongodb:[14,23,26],montydb:[14,23],more:[5,28],movi:[2,5,6,8,9],msg_md5sum:[1,4],msg_type:[1,4,6,18,21],multipl:[5,34],must:11,mutablemap:26,mysql:[7,21],name:[12,18,21,25,26,28,34,36],nan:[1,4,5,6],nativ:4,ndarrai:[5,28,32,33],need:4,next:5,nice:34,nomodelmatchederror:[5,27],none:[10,11,12,18,21,25,26,27,28,34],note:[4,5],now:4,num_job:12,number:[11,12,21,25,26],numpi:[5,27],object:[4,9,10,11,18,19,22,26,27,28,32,33,34],offset:[1,11,21,25,26],offset_cpu:34,offset_phys:34,one:[4,5,11,28,34],onli:4,open:34,open_csv:34,oper:[7,10,11,33],opt:[1,4,5,6],option:[11,21,25,26,28,34],order_bi:[11,18,21,25,26],orient:4,other:5,out:6,out_pkl:12,output:[12,36],output_db_engin:12,output_db_host:[12,36],output_db_nam:12,output_db_password:12,output_db_usernam:12,overview:7,overwrit:[11,26],packag:[7,8],page:[7,21,25,26],panda:[18,19,21,22,25,26],parallel:12,paramet:[11,12,18,19,21,22,25,26,27,28,32,33,34],pars:10,parsabl:[1,11],pass:[27,32],passthrough:[8,9],password:[12,18,21,25,26],path:[1,2,4,5,6,11,12,18,21,27,28,34],path_to_assign_list:34,path_to_csv:34,path_to_db:9,pcd:[9,28],per:[21,25,26],pick:12,pickl:12,pip3:7,pipe:1,pkl:9,pkl_file:12,pleas:4,poetri:[10,12],pointcloud2:4,pointcloud:[9,28],pointer:34,points_concat_downsampl:[1,4,6],pql:[1,4,5,11,25,26],prepar:34,preprocess:[8,9,27],previou:5,print:[4,5],prioriti:[2,28],proce:1,process:[7,10,32],properti:[4,18,19,21,22,26,27,28],provid:[0,4],pydtk:[0,1,3,4,5,6,7],python:[1,11,26],q_content:12,queri:[1,4,11,12,18,19,21,22,25,26,34],question:34,rang:6,rate:[28,32],raw_data:[1,4,5,6,28],read:[4,5,6,7,11,18,21,25,26,27],read_on_init:[4,18],read_sql_queri:[18,21,25,26],reader:[5,8,9,28],record:[2,3,4,5,6,7,9,11,25,26,34],record_id:[1,2,4,5,6,11,18,21],record_id_df:[18,21],record_id_list:34,refer:34,reformat:34,regardless:26,regex:[1,4,11,34],regist:[7,21,22,25,26,28],register_engin:[22,25],register_handl:[21,26],register_model:28,regular:4,rel:12,relat:[2,3,10,11],relationship:7,remov:[18,21,25,26,34],remove_data:26,remove_dupl:[18,21],remove_fil:26,remove_record:26,represent:28,resolut:34,resolution_com:34,resourc:[7,11],respect:36,result:[1,4,19,22],retriev:[4,5,7,10],right:7,risk_annot:[1,4],risk_factor:4,risk_scor:[1,4],root:[5,6,18],ros:7,rosbag:[1,4,5,6,8,9],rosbag_model_test:[1,6],row:[18,21,26],same:32,sampl:[1,4,5,6,28,32],save:[12,18,21,25,26,27,28],scene_descript:4,scope:4,script:[10,12],search:[1,5,7,9,11,12,19,22,34],search_cont:34,search_engin:[14,17,20],sec:[12,28,32,33],see:4,select:[7,18,19,21,22,25,26,27,28],self:[18,21,28],sensor_msg:4,sequenc:32,seri:[18,21,22],serial:34,serialize_dict_1d:34,set:[25,28,36],show:[9,10,11],sign:34,signal:[5,32,33,34],signal_list:34,simpl:5,singl:34,size:12,skip:[11,32],skip_checking_exist:11,smart_open:34,some:12,sort:[11,18,21,25,26],span:[12,19,33],specif:[1,4,26],specifi:[1,2,25],sql:[18,19,21,22,26],sqlalchemi:[18,21,26],sqlite:[18,21,36],srt:[18,21,26],stackoverflow:34,start:[12,27,28,34],start_tim:34,start_timestamp:[1,2,4,5,6,18,21,27,28,34],stat_data:33,statist:[8,9,12,14,18,19,20],statistic_db:[8,9],statistic_t:33,statisticscassandradbhandl:21,statisticsdbhandl:21,statu:[9,10],stdin:11,stdout:34,storag:25,storageproxi:25,store:[4,5,12,34],str:[1,2,5,11,12,18,19,21,22,25,26,27,28,33,34],strategi:26,stream:34,string:[1,11,18,19,21,22,26,28,34],sub:11,sub_command:[9,10],sub_record_id:4,subject:4,submodul:[8,15,17,20,23],subpackag:8,support:[4,7,21,26,28],sync_timestamp:33,sys:34,tabl:[12,18,21,25,26,33,34,36],table_nam:25,tag:[1,2,4,5,6,12,18,21,28,34],tag_filt:34,tag_list:34,take:4,take_time_and:34,target:[11,12,25,26,28,32,33,34,36],target_dir:12,target_frame_r:[28,32],target_span:33,templat:[7,10,11],test123:6,test1:[1,4],test2:[1,4],test:[1,3,4,5,6,7,11],test_db:6,test_db_2:6,text:[1,4,5,6,28],than:5,therefor:4,thi:[4,5,11,25,26,28],those:5,three:7,through:32,thu:5,time:[4,18,21,22,34],time_in_nsec:34,time_seri:[14,17,20],timeseriescassandradbhandl:21,timeseriescassandradbsearchengin:22,timeseriesdb:19,timeseriesdbhandl:[18,19,21],timeseriesdbsearchengin:19,timestamp:[1,4,5,6,27,28,32,33],tinydb:[14,23,26],tinymongo:[4,14,23,26],tinymongocollect:25,tmp:[18,21,26],to_dict:28,to_ndarrai:28,to_sql:[18,21],to_str:28,tool:[7,10,12],toolkit:[4,10,18,21],total:[21,25,26],transpar:34,treat:4,two:34,type:[1,2,6,7,9,12,18,19,21,22,25,26,27,28,32,33,34],uniqu:25,unit:[4,34],unless:26,unpack_data:34,unregular:34,unsupport:[28,33],unsupportedfileerror:28,unsupportedoperationerror:33,upsert:25,use:[1,4,5,11],used:[4,28,34,36],usernam:[12,18,21,25,26],using:[1,7,11,21],util:[8,9],uuid:25,v2db:17,v2dbhandler:18,v3db:20,v3dbhandler:21,v3timeseriescassandradb:22,v4db:23,v4dbhandler:[4,25,26],valid:14,valu:[25,28,32,34,36],variabl:11,vehicl:[1,6,19,22],venv:[1,2,3,36],verbos:12,veri:4,version:10,visual:4,want:[4,7],warn:[5,6],well:4,what:11,when:4,where:[12,18,21,26,34,36],which:[4,5,12,27,34,36],work:[5,12],write:[7,25,27],write_point:18,writer:[8,9],you:[0,1,4,5,7,35,36],zstd:[9,28]},titles:["<span class=\"section-number\">1. </span>Overview","<span class=\"section-number\">2. </span>DB Operations","<span class=\"section-number\">3. </span>Model Operations","<span class=\"section-number\">4. </span>IO Operations","<span class=\"section-number\">1.1. </span>Example 1: Grab metadata from a database","<span class=\"section-number\">1.2. </span>Example 2: Grab data based on metadata","Save metadata to database from json file","Welcome to Dataware Toolkit\u2019s documentation!","pydtk","pydtk package","pydtk.bin package","pydtk.bin.sub_commands package","pydtk.builder package","pydtk.builder.dbdb package","pydtk.db package","pydtk.db.v1 package","pydtk.db.v1.handlers package","pydtk.db.v2 package","pydtk.db.v2.handlers package","pydtk.db.v2.search_engines package","pydtk.db.v3 package","pydtk.db.v3.handlers package","pydtk.db.v3.search_engines package","pydtk.db.v4 package","pydtk.db.v4.deps package","pydtk.db.v4.engines package","pydtk.db.v4.handlers package","pydtk.io package","pydtk.models package","pydtk.models.autoware package","pydtk.models.pointcloud package","pydtk.models.zstd package","pydtk.preprocesses package","pydtk.statistics package","pydtk.utils package","<span class=\"section-number\">1. </span>Try pydtk","<span class=\"section-number\">2. </span>Register an existing dataset"],titleterms:{"try":35,IDs:4,access:4,add:1,api:7,autowar:29,avail:2,base:5,bin:[10,11],builder:[12,13],calcul:33,can:2,can_decod:34,can_packet:29,check:6,cli:[7,10],concat_df:12,concept:7,content:[1,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34],correctli:6,csv:28,data:5,databas:[1,4,6],databs:6,dataset:36,datawar:7,dbdb:13,delet:1,dep:24,df_list:12,df_path:12,dict:6,document:7,downsampl:32,engin:25,error:27,exampl:[4,5],except:14,exist:36,file:[1,2,3,6],file_info:13,fileutil:34,filter:5,from:[2,4,6],frontend:9,gener:2,get:[1,4],grab:[4,5],handler:[16,18,21,26],imag:28,indic:7,instal:7,iter:5,json:6,json_model:28,list:[1,2,4],make_meta:10,meta:[16,18,21,26],meta_db:12,metadata:[2,4,5,6],model:[2,11,28,29,30,31],modul:[9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34],mongodb:25,montydb:25,movi:28,notebook:35,oper:[1,2,3],overview:0,packag:[9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34],passthrough:32,pcd:30,pointcloud:30,preprocess:32,pydtk:[2,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35],quick:7,read:[2,3],reader:27,record:1,refer:7,regist:36,resourc:1,rosbag:[28,31],save:6,search:4,search_engin:[19,22],start:7,statist:[21,33],statistic_db:12,statu:11,store:6,sub_command:11,submodul:[9,10,11,12,13,14,16,18,19,21,22,25,26,27,28,29,30,31,32,33,34],subpackag:[9,10,12,14,15,17,20,23,28],tabl:7,templat:2,test:2,time_seri:[19,21,22],tinydb:25,tinymongo:25,toolkit:7,using:2,util:34,welcom:7,writer:27,zstd:31}})