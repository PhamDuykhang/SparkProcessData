package vn.edu.ctu.cit.thesis.dataprocess;

import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MutilayerPrediction implements Prediction {
    private MultilayerPerceptronClassificationModel model;
    @Override
    public Dataset<Row> makePrediction(Dataset<Row> data) {
        Dataset result = this.model.transform(data);
        return result;
    }

    @Override
    public void loadModel(String path) {
        this.model = MultilayerPerceptronClassificationModel.load(path);
        System.out.println(path +" Load success!");
    }
}
