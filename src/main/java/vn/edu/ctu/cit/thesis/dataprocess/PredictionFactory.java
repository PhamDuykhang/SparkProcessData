package vn.edu.ctu.cit.thesis.dataprocess;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PredictionFactory {
    Dataset<Row> getPrediction(String modeltype, String modelpath, Dataset<Row> data){
        if (modeltype==null){
           return null;
        }
        if(modeltype.equalsIgnoreCase("KMEAN")){

            Prediction prediction = new KmeanPrediction();
            prediction.loadModel(modelpath);
            return prediction.makePrediction(SparkUtil.normalizeFeture(data));
        }
        if(modeltype.equalsIgnoreCase("ONEVSALL")){
            Prediction prediction = new OneVsAllPrediction();
            prediction.loadModel(modelpath);
            return prediction.makePrediction(SparkUtil.normalizeFeture(data));
        }
        if(modeltype.equalsIgnoreCase("MUTILAYER")){
            Prediction prediction = new MutilayerPrediction();
            prediction.loadModel(modelpath);
            return prediction.makePrediction(SparkUtil.normalizeFeture(data));
        }
        return null;
    }
}
