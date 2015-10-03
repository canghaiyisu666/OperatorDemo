spark-submit --master yarn --name LowerUpperCaseConvert --class iie.udps.example.spark.LowerUpperCaseConvert --executor-memory 450M --jars $LIBJARS $OPJAR -c $CONF
