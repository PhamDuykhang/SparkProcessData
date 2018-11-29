package vn.edu.ctu.cit.thesis.dataprocess;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

public class SparkUtil {
    public static Dataset<Row> normalizeFeture(Dataset<Row> dataset){
        String[] arrayColFeature = {"Area","CentroidX","CentroidY", "Perimeter","DistanceWithSkull","Diameter"
                ,"Solidity", "BBULX","BBULY","BBWith","BBHeight","FilledArea","Extent", "Eccentricity", "MajorAxisLength"
                , "MinorAxisLength","Orientation"};
        VectorAssembler vector_assemble = new VectorAssembler()
                .setInputCols(arrayColFeature)
                .setOutputCol("vector_features");
        return vector_assemble.transform(dataset);
    }
    public static void writeToMysql(Dataset<Row> dataset,String tablename,String databaseaddress,String usename,String password){


    }


}
