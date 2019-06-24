package com.semantix.test;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Locale;


public class SparkSessionHandler {

    private SparkSession spark;

    private final SimpleDateFormat input = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private final SimpleDateFormat output = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);

    private SparkSessionHandler() {

        spark = SparkSession.builder().appName("nasa-spark").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

    }

    public static SparkSession getInstance(){
        return new SparkSessionHandler().spark;
    }
}
