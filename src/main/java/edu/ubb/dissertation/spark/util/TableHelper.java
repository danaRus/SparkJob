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
        return String.join(", ", "timestamp", "patient_id", "surgery_id", "systolicBloodPressure",
                "systolicBloodPressureUpperLimit", "systolicBloodPressureLowerLimit", "diastolicBloodPressure",
                "diastolicBloodPressureUpperLimit", "diastolicBloodPressureLowerLimit", "heartRate",
                "heartRateUpperLimit", "heartRateLowerLimit", "oxygenSaturationLevel", "oxygenSaturationLevelUpperLimit",
                "oxygenSaturationLevelLowerLimit", "bloodLossRate", "abnormalVitalSigns");
    }

    public static String createSensorDataColumns() {
        return String.join(", ", "timestamp", "rotation", "temperature", "force", "pressure", "arm_id");
    }
}
