package vn.edu.ctu.cit.thesis.dataprocess;

import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class KmeanPrediction implements Prediction {
    private KMeansModel model;
    @Override
    public void loadModel(String path) {
        this.model = KMeansModel.load(path);
        System.out.println(path +" Load success!");
    }

    @Override
    public Dataset<Row> makePrediction(Dataset<Row> data) {
        Dataset<Row> result = this.model.transform(data);
        return result;
    }
}
