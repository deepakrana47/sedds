spark-submit --masteryarn --deploy-mode cluster \
--py-files sdd_lib.zip \
--files conf/sdd.conf,conf/spark.conf,log4j.properties \
sdd_main.py qa 2024-04-23