package com.semantix.test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Locale;

import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        String filepath = "src/main/resources/*";
        String delimiter = " ";
        boolean header = false;

        SparkSession spark = SparkSessionHandler.getInstance();
        Dataset<Row> dataset = DatasetHandler.readFromCSVFile(spark, filepath, delimiter, header);

        SimpleDateFormat input = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        SimpleDateFormat output = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);

        spark.udf().register("dateExtract", (String dateStr) -> {
            dateStr = dateStr.replace('[',' ').trim();
            java.util.Date inputDate = input.parse(dateStr);
            return output.format(inputDate);
        }, DataTypes.StringType);

        spark.udf().register("dateLong", (String dateStr) -> {
            java.util.Date inputDate = output.parse(dateStr);
            return inputDate.getTime();
        }, DataTypes.LongType);

        spark.udf().register("zeroBytes", (String bytes) -> {
            try{
                long val = Long.parseLong(bytes);
                return val;
            }
            catch (Exception e){
                return 0L;
            }
        }, DataTypes.LongType);

        dataset = DatasetHandler.cleanAndRenameColumns(dataset);
        dataset = dataset.withColumn("day", callUDF("dateExtract",(col("timestamp"))))
                .withColumn("longTime", callUDF("dateLong", col("day")));


        Dataset<Row> dataset404 = DatasetHandler.filter404(dataset);

        long hostsNum = DatasetHandler.getUniquehostsNumber(dataset);
        long notFoundNum = dataset404.count();
        Dataset<Row> totalOfBytes = DatasetHandler.getTotalOfBytes(dataset);


        Dataset<Row> aggData = DatasetHandler.orderByHostRequestsDesc(dataset404);
        Dataset<Row> aggDataByDay = DatasetHandler.aggregateErrorsByDay(dataset404);

        System.out.println("Unique hosts: "+hostsNum);
        System.out.println("404: "+notFoundNum);
        System.out.println(" ");
        System.out.println("5 URLs que mais causaram erro 404:");
        aggData.show(5);
        System.out.println(" ");
        System.out.println("Quantidade de erros 404 por dia:");
        aggDataByDay.show(100);
        System.out.println(" ");
        System.out.println(" O total de bytes retornados:");
        totalOfBytes.show(100, false);

    }
}
