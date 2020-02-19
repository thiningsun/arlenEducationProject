/opt/cloudera/parcels/spark-2.1.1/spark-2.1.1-bin-2.6.0-cdh5.11.0/bin/spark-submit \
--name AnalyCreateTable \
--master local[2]  \
--class mso.metadata.AnalyCreateTable /workspace/business/spark-jar/business.mso-1.0-SNAPSHOT.jar