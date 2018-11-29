package vn.edu.ctu.cit.thesis.dataprocess;




import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;

import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.glassfish.jersey.message.WriterModel;


import java.util.*;

public class DicomProcessData {
    public static final String APP_NAME="Dicom process";
//    private static final String PCA_MODEL_LOCATON = "hdfs://localhost:9000/data/PCAmodel_new_v3.5_pca_5";
//    private static final String KMEAN_MODEL_LOCATON ="hdfs://localhost:9000/data/kmeanmodel_new_v3.5_pca_5";
//    private static final String DATABASE_URL = "jdbc:mysql://localhost:3306/";
//    private static final String DATABASE_NAME = "FINAL_RESUT";
//    private static final String TABLE_NAME = "Diagnostic_result";
    private static final StructType DicomFileDataSchema = new StructType(new StructField[]{
            new StructField("fileName", DataTypes.StringType, true, Metadata.empty()),
            new StructField("PatientID", DataTypes.StringType, true, Metadata.empty()),
            new StructField("PatientName", DataTypes.StringType, true, Metadata.empty()),
            new StructField("PatientAge", DataTypes.StringType, true, Metadata.empty()),
            new StructField("PatientSex", DataTypes.StringType, true, Metadata.empty()),
            new StructField("InstitutionName", DataTypes.StringType, true, Metadata.empty()),
            new StructField("institutionAddress", DataTypes.StringType, true, Metadata.empty()),
            new StructField("AccessionNumber", DataTypes.StringType, true, Metadata.empty()),
            new StructField("Manufacturer", DataTypes.StringType, true, Metadata.empty()),
            new StructField("Modality", DataTypes.StringType, true, Metadata.empty()),
            new StructField("Area", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("CentroidX", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("CentroidY", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("Perimeter", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("DistanceWithSkull", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("Diameter", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("Solidity", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("BBULX", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("BBULY", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("BBWith", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("BBHeight", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("FilledArea", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("Extent", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("Eccentricity", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("MajorAxisLength", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("MinorAxisLength", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("Orientation", DataTypes.FloatType, true, Metadata.empty()),

    });
    public static void main(String[] args) throws InterruptedException {
        String modeltype = args[0];
        String modelpath=args[1];
        String hdfs_url = args[2];
        String kafka_host_list = args[3];
        String kafka_topic= args[4];
        int durantion = Integer.valueOf(args[5]);
        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf,Durations.seconds(durantion));
        UDF1<Double,String> cover_result = new UDF1<Double, String>() {
            @Override
            public String call(Double prediction) throws Exception {
                if (prediction==0.0){
                    return "Duoi Nhen";
                }
                if(prediction==1.0) {
                    return "Mau Tu Duoi Mang Cung";
                }
                if(prediction==2.0) {
                    return "Mau Tu Ngoai Mang Cung";
                }

                if(prediction==3.0) {
                    return "Chay Mau Noi So";
                }
                return "Null";
            }
        };

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers",kafka_host_list);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "dicomprocess");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList(kafka_topic.split(","));
        System.out.println("Result");
        JavaInputDStream<ConsumerRecord<String,String>> stream =KafkaUtils.createDirectStream(
                javaStreamingContext
                ,LocationStrategies.PreferConsistent()
                ,ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams));
        JavaDStream<String> dStream=stream.map(line->line.value());
        Properties conectionproperties = new Properties();
        conectionproperties.put("user","root");
        conectionproperties.put("password","");
        String[] arrayColFeature = {"Area","CentroidX","CentroidY", "Perimeter","DistanceWithSkull","Diameter"
                ,"Solidity", "BBULX","BBULY","BBWith","BBHeight","FilledArea","Extent", "Eccentricity", "MajorAxisLength"
                , "MinorAxisLength","Orientation"};
        VectorAssembler vector_assemble = new VectorAssembler()
                .setInputCols(arrayColFeature)
                .setOutputCol("vector_features");
        dStream.foreachRDD((rdd,time )-> {
            SparkSession spark_session = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
            spark_session.udf().register("cover_result",cover_result,DataTypes.StringType);
            if(!rdd.isEmpty()){
                System.out.println("Nhan duoc du lieu");
                long startTime = System.nanoTime();
                long count = rdd.count();
                System.out.println("At "+time+" recive: "+count+" data");
                Dataset<Row> df = spark_session.read().schema(DicomFileDataSchema).json(rdd);
                Dataset<Row> data = vector_assemble.transform(df);
                Dataset<Row> result;
                System.out.println("Data recive");
                if(modeltype.equalsIgnoreCase("ONEVSALL")){
                    OneVsRestModel model= OneVsRestModel.load(modelpath);
                    result = model.setFeaturesCol("vector_features").transform(data);
                    result= result.withColumn("diagnostic_results",functions.callUDF("cover_result",result.col("prediction")));
                    result.select("fileName","PatientID","PatientName","PatientAge","InstitutionName","diagnostic_results").show();
                    result.drop("vector_features").write().mode(SaveMode.Append).save("hdfs://"+hdfs_url);
                    long endTime = System.nanoTime();
					System.out.println("Save success!!");
                    long timeElapsed = endTime - startTime;
                    System.out.println("Thoi gian thuc hien : "+timeElapsed/1000000000);
                }
                if(modeltype.equalsIgnoreCase("MLP")){
                    MultilayerPerceptronClassificationModel model;
                    model = MultilayerPerceptronClassificationModel.load(modelpath);
                    result = model.setFeaturesCol("vector_features").transform(data);
                    result= result.withColumn("diagnostic_results",functions.callUDF("cover_result",result.col("prediction")));
                    result.select("fileName","PatientID","PatientName","PatientAge","InstitutionName","diagnostic_results").show();
                    result.drop("vector_features").write().mode(SaveMode.Append).save("hdfs://"+hdfs_url);
					System.out.println("Save success!!");
                }
            }
            System.out.println("No data!!!!!!!!");
        });
javaStreamingContext.start();
javaStreamingContext.awaitTermination();

    }
}
