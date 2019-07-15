package edu.ubb.dissertation.spark.util;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class TableHelper {

    private TableHelper() {
    }

    public static String createMergedDataTableColumnsWithType() {
        final Stream<String> timestampField = Stream.of("timestamp TIMESTAMP");
        final Stream<String> patientDoubleFields = Stream.of("systolicBloodPressure", "systolicBloodPressureUpperLimit",
                "systolicBloodPressureLowerLimit", "diastolicBloodPressure", "diastolicBloodPressureUpperLimit",
                "diastolicBloodPressureLowerLimit", "heartRate", "heartRateUpperLimit", "heartRateLowerLimit",
                "oxygenSaturationLevel", "oxygenSaturationLevelUpperLimit", "oxygenSaturationLevelLowerLimit", "bloodLossRate",
                "systolicBloodPressureType", "diastolicBloodPressureType", "heartRateType", "oxygenSaturationLevelType")
                .map(value -> String.format("%s DOUBLE", value));
        final Stream<String> patientIdFields = Stream.of("patient_id INTEGER",
                "surgery_id STRING");
        final Stream<String> sensorDoubleFields = Stream.of("rotation", "temperature", "force", "pressure")
                .map(value -> String.format("%s DOUBLE", value));
        final Stream<String> armIdField = Stream.of("arm_id STRING");
        return Stream.of(timestampField, patientDoubleFields, patientIdFields, sensorDoubleFields, armIdField)
                .flatMap(value -> value)
                .collect(Collectors.joining(", "));
    }

    public static String[] createMergedDataTableColumns() {
        return Stream.of("timestamp", "systolicBloodPressure", "systolicBloodPressureUpperLimit",
                "systolicBloodPressureLowerLimit", "diastolicBloodPressure", "diastolicBloodPressureUpperLimit",
                "diastolicBloodPressureLowerLimit", "heartRate", "heartRateUpperLimit", "heartRateLowerLimit",
                "oxygenSaturationLevel", "oxygenSaturationLevelUpperLimit", "oxygenSaturationLevelLowerLimit",
                "bloodLossRate", "systolicBloodPressureType", "diastolicBloodPressureType", "heartRateType",
                "oxygenSaturationLevelType", "patientId", "surgeryId",
                "rotation", "temperature", "force", "pressure", "armId")
                .toArray(String[]::new);
    }


    public static String createPatientDataColumns() {
        return String.join(", ", "timestamp", "patient_id", "surgery_id", "systolic_blood_pressure",
                "systolic_blood_pressure_upper_limit", "systolic_blood_pressure_lower_limit", "diastolic_blood_pressure",
                "diastolic_blood_pressure_upper_limit", "diastolic_blood_pressure_lower_limit", "heart_rate",
                "heart_rate_upper_limit", "heart_rate_lower_limit", "oxygen_saturation_level", "oxygen_saturation_level_upper_limit",
                "oxygen_saturation_level_lower_limit", "blood_loss_rate", "abnormal_vital_signs");
    }

    public static String createSensorDataColumns() {
        return String.join(", ", "timestamp", "rotation", "temperature", "force", "pressure", "arm_id");
    }
}
