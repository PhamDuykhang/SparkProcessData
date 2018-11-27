package vn.edu.ctu.cit.thesis.dataprocess;

import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class OneVsAllPrediction implements Prediction {
    private OneVsRestModel mode;
    @Override
    public Dataset<Row> makePrediction(Dataset<Row> data) {
        Dataset<Row> result = mode.transform(data);
        return result;
    }

    @Override
    public void loadModel(String path) {
        this.mode = OneVsRestModel.load(path);
    }
}
