package vn.edu.ctu.cit.thesis.dataprocess;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StrucTypeUtil {
    public static StructType getStructype(){
        StructType DicomFileDataSchema = new StructType(new StructField[]{
                new StructField("PatientID",DataTypes.StringType,true,Metadata.empty()),
                new StructField("PatientName",DataTypes.StringType,true,Metadata.empty()),
                new StructField("PatientAge",DataTypes.StringType,true,Metadata.empty()),
                new StructField("PatientSex",DataTypes.StringType,true,Metadata.empty()),
                new StructField("InstitutionName",DataTypes.StringType,true,Metadata.empty()),
                new StructField("institutionAddress",DataTypes.StringType,true,Metadata.empty()),
                new StructField("AccessionNumber",DataTypes.StringType,true,Metadata.empty()),
                new StructField("Manufacturer",DataTypes.StringType,true,Metadata.empty()),
                new StructField("Modality",DataTypes.StringType,true,Metadata.empty()),
                new StructField("Area",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("CentroidX",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("CentroidY",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("Perimeter",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("DistanceWithSkull",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("EquivDiameter",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("AreaBoudingbox",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("Solidity",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("Extent",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("Eccentricity",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("MajorAxisLength",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("MinorAxisLength",DataTypes.FloatType,true,Metadata.empty()),
                new StructField("Orientation",DataTypes.FloatType,true,Metadata.empty()),
        });
        return DicomFileDataSchema;
    }
}
