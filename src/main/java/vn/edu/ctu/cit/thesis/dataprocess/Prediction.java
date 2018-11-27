package vn.edu.ctu.cit.thesis.dataprocess;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Prediction {
     Dataset<Row> makePrediction(Dataset<Row> data);
     void loadModel(String path);
}
