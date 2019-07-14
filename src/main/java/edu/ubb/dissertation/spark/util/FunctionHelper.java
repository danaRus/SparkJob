package edu.ubb.dissertation.spark.util;

import edu.ubb.dissertation.spark.model.PatientData;
import edu.ubb.dissertation.spark.model.SensorData;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.time.LocalDateTime;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FunctionHelper {

    private FunctionHelper() {
    }

    public static FilterFunction<PatientData> createPatientDataFilterFunction(final LocalDateTime startTimestamp,
                                                                              final LocalDateTime endTimestamp) {
        return patientData -> patientData.getTimestamp().isAfter(startTimestamp)
                && patientData.getTimestamp().isBefore(endTimestamp);
    }

    public static MapFunction<Row, PatientData> createPatientMapFunction() {
        return row -> new PatientData.Builder()
                .withTimestamp(row.getTimestamp(0).toLocalDateTime())
                .withPatientId(row.getLong(1))
                .withSurgeryId(row.getString(2))
                .withSystolicBloodPressure(row.getDouble(3))
                .withSystolicBloodPressureUpperLimit(row.getDouble(4))
                .withSystolicBloodPressureLowerLimit(row.getDouble(5))
                .withDiastolicBloodPressure(row.getDouble(6))
                .withDiastolicBloodPressureUpperLimit(row.getDouble(7))
                .withDiastolicBloodPressureLowerLimit(row.getDouble(8))
                .withHeartRate(row.getDouble(9))
                .withHeartRateUpperLimit(row.getDouble(10))
                .withHeartRateLowerLimit(row.getDouble(11))
                .withOxygenSaturationLevel(row.getDouble(12))
                .withOxygenSaturationLevelUpperLimit(row.getDouble(13))
                .withOxygenSaturationLevelLowerLimit(row.getDouble(14))
                .withBloodLossRate(row.getDouble(15))
                .withAbnormalVitalSigns(Stream.of(row.getString(16)).collect(Collectors.toList()))
                .build();
    }

    public static FilterFunction<SensorData> createSensorDataFilterFunction(final LocalDateTime startTimestamp
            , final LocalDateTime endTimestamp) {
        return sensorData -> sensorData.getTimestamp().isAfter(startTimestamp)
                && sensorData.getTimestamp().isBefore(endTimestamp);
    }

    public static MapFunction<Row, SensorData> createSensorMapFunction() {
        return row -> new SensorData.Builder()
                .withTimestamp(row.getTimestamp(0).toLocalDateTime())
                .withRotation(row.getDouble(1))
                .withTemperature(row.getDouble(2))
                .withForce(row.getDouble(3))
                .withPressure(row.getDouble(4))
                .withArmId(row.getString(5))
                .build();
    }
}
