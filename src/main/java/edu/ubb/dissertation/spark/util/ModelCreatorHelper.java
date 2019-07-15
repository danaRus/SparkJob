package edu.ubb.dissertation.spark.util;

import edu.ubb.dissertation.spark.model.MergedData;
import edu.ubb.dissertation.spark.model.PatientData;
import edu.ubb.dissertation.spark.model.SensorData;
import org.apache.spark.sql.Row;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ModelCreatorHelper {

    private ModelCreatorHelper() {
    }

    public static PatientData createPatientDataFromRow(final Row row) {
        return new PatientData.Builder()
                .withTimestamp(row.getTimestamp(0).toLocalDateTime())
                .withPatientId((long) row.getInt(1))
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

    public static SensorData createSensorDataFromRow(final Row row) {
        return new SensorData.Builder()
                .withTimestamp(row.getTimestamp(0).toLocalDateTime())
                .withRotation(row.getDouble(1))
                .withTemperature(row.getDouble(2))
                .withForce(row.getDouble(3))
                .withPressure(row.getDouble(4))
                .withArmId(row.getString(5))
                .build();
    }

    public static PatientData createPatientData(final PatientData patientData, final List<String> abnormalVitalSigns) {
        return new PatientData.Builder()
                .withTimestamp(patientData.getTimestamp())
                .withSystolicBloodPressure(patientData.getSystolicBloodPressure())
                .withSystolicBloodPressureUpperLimit(patientData.getSystolicBloodPressureUpperLimit())
                .withSystolicBloodPressureLowerLimit(patientData.getSystolicBloodPressureLowerLimit())
                .withDiastolicBloodPressure(patientData.getDiastolicBloodPressure())
                .withDiastolicBloodPressureUpperLimit(patientData.getDiastolicBloodPressureUpperLimit())
                .withDiastolicBloodPressureLowerLimit(patientData.getDiastolicBloodPressureLowerLimit())
                .withHeartRate(patientData.getHeartRate())
                .withHeartRateUpperLimit(patientData.getHeartRateUpperLimit())
                .withHeartRateLowerLimit(patientData.getHeartRateLowerLimit())
                .withOxygenSaturationLevel(patientData.getOxygenSaturationLevel())
                .withOxygenSaturationLevelUpperLimit(patientData.getOxygenSaturationLevelUpperLimit())
                .withOxygenSaturationLevelLowerLimit(patientData.getOxygenSaturationLevelLowerLimit())
                .withBloodLossRate(patientData.getBloodLossRate())
                .withAbnormalVitalSigns(abnormalVitalSigns)
                .build();
    }

    public static Optional<MergedData> createMergedData(final PatientData patientData, final SensorData sensorData) {
        return Objects.nonNull(patientData) && Objects.nonNull(sensorData)
                ? Optional.of(createData(patientData, sensorData))
                : Optional.empty();
    }

    private static MergedData createData(final PatientData patientData, final SensorData sensorData) {
        return new MergedData.Builder()
                .withTimestamp(Timestamp.valueOf(patientData.getTimestamp()))
                .withSystolicBloodPressure(patientData.getSystolicBloodPressure())
                .withSystolicBloodPressureUpperLimit(patientData.getSystolicBloodPressureUpperLimit())
                .withSystolicBloodPressureLowerLimit(patientData.getSystolicBloodPressureLowerLimit())
                .withDiastolicBloodPressure(patientData.getDiastolicBloodPressure())
                .withDiastolicBloodPressureUpperLimit(patientData.getDiastolicBloodPressureUpperLimit())
                .withDiastolicBloodPressureLowerLimit(patientData.getDiastolicBloodPressureLowerLimit())
                .withHeartRate(patientData.getHeartRate())
                .withHeartRateUpperLimit(patientData.getHeartRateUpperLimit())
                .withHeartRateLowerLimit(patientData.getHeartRateLowerLimit())
                .withOxygenSaturationLevel(patientData.getOxygenSaturationLevel())
                .withOxygenSaturationLevelUpperLimit(patientData.getOxygenSaturationLevelUpperLimit())
                .withOxygenSaturationLevelLowerLimit(patientData.getOxygenSaturationLevelLowerLimit())
                .withBloodLossRate(patientData.getBloodLossRate())
                .withSystolicBloodPressureType(patientData.getSystolicBloodPressureType())
                .withDiastolicBloodPressureType(patientData.getDiastolicBloodPressureType())
                .withHeartRateType(patientData.getHeartRateType())
                .withOxygenSaturationLevelType(patientData.getOxygenSaturationLevelType())
                .withPatientId(patientData.getPatientId())
                .withSurgeryId(patientData.getSurgeryId())
                .withRotation(sensorData.getRotation())
                .withTemperature(sensorData.getTemperature())
                .withForce(sensorData.getForce())
                .withPressure(sensorData.getPressure())
                .withArmId(sensorData.getArmId())
                .build();
    }
}
