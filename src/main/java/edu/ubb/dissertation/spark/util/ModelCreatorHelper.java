package edu.ubb.dissertation.spark.util;

import edu.ubb.dissertation.spark.model.MergedData;
import edu.ubb.dissertation.spark.model.PatientData;
import edu.ubb.dissertation.spark.model.SensorData;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class ModelCreatorHelper {

    private ModelCreatorHelper() {
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
