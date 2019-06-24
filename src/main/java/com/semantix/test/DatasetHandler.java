package com.semantix.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class DatasetHandler {

    public static Dataset<Row> readFromCSVFile(SparkSession spark, String filepath, String delimiter, boolean header){

        Dataset<Row> dataset = spark.read().option("header", header)
                .option("delimiter", delimiter)
                .csv(filepath);

        return dataset;
    }

    public static Dataset<Row> cleanAndRenameColumns(Dataset<Row> dataset){

        return  dataset
                    .withColumnRenamed("_c0", "host")
                    .withColumnRenamed("_c3", "timestamp")
                    .withColumnRenamed("_c4", "timezone")
                    .withColumnRenamed("_c5", "requisicao")
                    .withColumnRenamed("_c6", "retorno")
                    .withColumnRenamed("_c7", "bytes")
                    .drop(col("_c1"))
                    .drop(col("_c2"))
                    .withColumn("bytes", callUDF("zeroBytes", col("bytes")));
    }

    public static Dataset<Row> orderByHostRequestsDesc(Dataset<Row> dataset){

        return  dataset
                    .groupBy("host")
                    .agg(count("host").name("noReqs"))
                    .orderBy(col("noReqs").desc());
    }

    public static Dataset<Row> filter404(Dataset<Row> dataset){

        return  dataset.filter(col("retorno").equalTo(404));
    }


    public static long getUniquehostsNumber(Dataset<Row> dataset){

        return  dataset
                    .select("host")
                    .distinct()
                    .count();
    }

    public static Dataset<Row> aggregateErrorsByDay(Dataset<Row> dataset) {
        return  dataset.select(col("day"), col("requisicao"), col("longTime"))
                    .groupBy("day", "longTime")
                    .agg(count("requisicao").name("no404"))
                    .orderBy(col("longTime"))
                    .drop("longTime");
    }

    public static Dataset<Row> getTotalOfBytes(Dataset<Row> dataset) {
        return dataset.select(col("bytes")).agg(sum(col("bytes")));
    }
}
