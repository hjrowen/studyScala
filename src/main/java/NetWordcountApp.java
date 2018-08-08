import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function;
// Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
// Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
// Import Row.
import org.apache.spark.sql.Row;
// Import RowFactory.
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NetWordcountApp {

    class Emp {




    }
    static StructType getType() {
        return DataTypes.createStructType(Arrays.asList((DataTypes.createStructField("name", DataTypes.StringType, true)),
                (DataTypes.createStructField("age", DataTypes.IntegerType, true)),
                (DataTypes.createStructField("salar", DataTypes.IntegerType, true))));
    }
    public static void main(String[] args) {
        try {
            SparkSession ss = new SparkSession.Builder().master("local[*]").appName("NetWordcountApp").getOrCreate();
            //SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetWordcountApp");
            JavaSparkContext sc=JavaSparkContext.fromSparkContext(ss.sparkContext());
            JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(2));


            JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop001", 9999);
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

            // Count each word in each batch
            JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
            JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

            JavaDStream<String> jpds = lines.window(Durations.seconds(10), Durations.seconds(2));
            lines.print();
            lines.foreachRDD(rdd -> {
                Row NUL = RowFactory.create(0);
                JavaRDD<Row> rowRDD = rdd.map(record-> {
                    String[] fields = record.split(" ");
                    if (fields.length != 3)
                        return NUL;
                    else {
                        return RowFactory.create(fields[0], Integer.valueOf(fields[1].trim()),  Integer.valueOf(fields[2].trim()));
                        //return RowFactory.create(fields[0], fields[1].trim(), fields[2].trim());
                    }
                });//.filter(x -> x != NUL);
                Dataset<Row> empDataFrame = ss.createDataFrame(rowRDD, getType());
                empDataFrame.createOrReplaceTempView("emp");
                ss.sql("SELECT name, sum(salar) FROM emp group by name").show();
            });

            jssc.start();

            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
