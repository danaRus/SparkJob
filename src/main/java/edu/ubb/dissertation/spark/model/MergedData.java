package edu.ubb.dissertation.spark.model;

import java.sql.Timestamp;

import static org.apache.commons.lang3.Validate.notNull;

public class MergedData {

    private Timestamp timestamp;
    // patient data fields
    private Double systolicBloodPressure;
    private Double systolicBloodPressureUpperLimit;
    private Double systolicBloodPressureLowerLimit;
    private Double diastolicBloodPressure;
    private Double diastolicBloodPressureUpperLimit;
    private Double diastolicBloodPressureLowerLimit;
    private Double heartRate;
    private Double heartRateUpperLimit;
    private Double heartRateLowerLimit;
    private Double oxygenSaturationLevel;
    private Double oxygenSaturationLevelUpperLimit;
    private Double oxygenSaturationLevelLowerLimit;
    private Double bloodLossRate;
    private Integer systolicBloodPressureType;
    private Integer diastolicBloodPressureType;
    private Integer heartRateType;
    private Integer oxygenSaturationLevelType;
    private Long patientId;
    private String surgeryId;
    // sensor data fields
    private Double rotation;
    private Double temperature;
    private Double force;
    private Double pressure;
    private String armId;

    public MergedData() {
    }

    private MergedData(final Builder builder) {
        this.timestamp = builder.timestamp;
        this.systolicBloodPressure = builder.systolicBloodPressure;
        this.systolicBloodPressureUpperLimit = builder.systolicBloodPressureUpperLimit;
        this.systolicBloodPressureLowerLimit = builder.systolicBloodPressureLowerLimit;
        this.diastolicBloodPressure = builder.diastolicBloodPressure;
        this.diastolicBloodPressureUpperLimit = builder.diastolicBloodPressureUpperLimit;
        this.diastolicBloodPressureLowerLimit = builder.diastolicBloodPressureLowerLimit;
        this.heartRate = builder.heartRate;
        this.heartRateUpperLimit = builder.heartRateUpperLimit;
        this.heartRateLowerLimit = builder.heartRateLowerLimit;
        this.oxygenSaturationLevel = builder.oxygenSaturationLevel;
        this.oxygenSaturationLevelUpperLimit = builder.oxygenSaturationLevelUpperLimit;
        this.oxygenSaturationLevelLowerLimit = builder.oxygenSaturationLevelLowerLimit;
        this.bloodLossRate = builder.bloodLossRate;
        this.systolicBloodPressureType = builder.systolicBloodPressureType;
        this.diastolicBloodPressureType = builder.diastolicBloodPressureType;
        this.heartRateType = builder.heartRateType;
        this.oxygenSaturationLevelType = builder.oxygenSaturationLevelType;
        this.patientId = builder.patientId;
        this.surgeryId = builder.surgeryId;
        // sensor data fields
        this.rotation = builder.rotation;
        this.temperature = builder.temperature;
        this.force = builder.force;
        this.pressure = builder.pressure;
        this.armId = builder.armId;
    }

    public static class Builder {
        private Timestamp timestamp;
        // patient data fields
        private Double systolicBloodPressure;
        private Double systolicBloodPressureUpperLimit;
        private Double systolicBloodPressureLowerLimit;
        private Double diastolicBloodPressure;
        private Double diastolicBloodPressureUpperLimit;
        private Double diastolicBloodPressureLowerLimit;
        private Double heartRate;
        private Double heartRateUpperLimit;
        private Double heartRateLowerLimit;
        private Double oxygenSaturationLevel;
        private Double oxygenSaturationLevelUpperLimit;
        private Double oxygenSaturationLevelLowerLimit;
        private Double bloodLossRate;
        private Integer systolicBloodPressureType;
        private Integer diastolicBloodPressureType;
        private Integer heartRateType;
        private Integer oxygenSaturationLevelType;
        private Long patientId;
        private String surgeryId;
        // sensor data fields
        private Double rotation;
        private Double temperature;
        private Double force;
        private Double pressure;
        private String armId;

        public Builder withTimestamp(final Timestamp timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder withSystolicBloodPressure(final Double systolicBloodPressure) {
            this.systolicBloodPressure = systolicBloodPressure;
            return this;
        }

        public Builder withSystolicBloodPressureUpperLimit(final Double systolicBloodPressureUpperLimit) {
            this.systolicBloodPressureUpperLimit = systolicBloodPressureUpperLimit;
            return this;
        }

        public Builder withSystolicBloodPressureLowerLimit(final Double systolicBloodPressureLowerLimit) {
            this.systolicBloodPressureLowerLimit = systolicBloodPressureLowerLimit;
            return this;
        }

        public Builder withDiastolicBloodPressure(final Double diastolicBloodPressure) {
            this.diastolicBloodPressure = diastolicBloodPressure;
            return this;
        }

        public Builder withDiastolicBloodPressureUpperLimit(final Double diastolicBloodPressureUpperLimit) {
            this.diastolicBloodPressureUpperLimit = diastolicBloodPressureUpperLimit;
            return this;
        }

        public Builder withDiastolicBloodPressureLowerLimit(final Double diastolicBloodPressureLowerLimit) {
            this.diastolicBloodPressureLowerLimit = diastolicBloodPressureLowerLimit;
            return this;
        }

        public Builder withHeartRate(final Double heartRate) {
            this.heartRate = heartRate;
            return this;
        }

        public Builder withHeartRateUpperLimit(final Double heartRateUpperLimit) {
            this.heartRateUpperLimit = heartRateUpperLimit;
            return this;
        }

        public Builder withHeartRateLowerLimit(final Double heartRateLowerLimit) {
            this.heartRateLowerLimit = heartRateLowerLimit;
            return this;
        }

        public Builder withOxygenSaturationLevel(final Double oxygenSaturationLevel) {
            this.oxygenSaturationLevel = oxygenSaturationLevel;
            return this;
        }

        public Builder withOxygenSaturationLevelUpperLimit(final Double oxygenSaturationLevelUpperLimit) {
            this.oxygenSaturationLevelUpperLimit = oxygenSaturationLevelUpperLimit;
            return this;
        }

        public Builder withOxygenSaturationLevelLowerLimit(final Double oxygenSaturationLevelLowerLimit) {
            this.oxygenSaturationLevelLowerLimit = oxygenSaturationLevelLowerLimit;
            return this;
        }

        public Builder withBloodLossRate(final Double bloodLossRate) {
            this.bloodLossRate = bloodLossRate;
            return this;
        }

        public Builder withSystolicBloodPressureType(final Integer systolicBloodPressureType) {
            this.systolicBloodPressureType = systolicBloodPressureType;
            return this;
        }

        public Builder withDiastolicBloodPressureType(final Integer diastolicBloodPressureType) {
            this.diastolicBloodPressureType = diastolicBloodPressureType;
            return this;
        }

        public Builder withHeartRateType(final Integer heartRateType) {
            this.heartRateType = heartRateType;
            return this;
        }

        public Builder withOxygenSaturationLevelType(final Integer oxygenSaturationLevelType) {
            this.oxygenSaturationLevelType = oxygenSaturationLevelType;
            return this;
        }

        public Builder withPatientId(final Long patientId) {
            this.patientId = patientId;
            return this;
        }

        public Builder withSurgeryId(final String surgeryId) {
            this.surgeryId = surgeryId;
            return this;
        }

        public Builder withRotation(final Double rotation) {
            this.rotation = rotation;
            return this;
        }

        public Builder withTemperature(final Double temperature) {
            this.temperature = temperature;
            return this;
        }

        public Builder withForce(final Double force) {
            this.force = force;
            return this;
        }

        public Builder withPressure(final Double pressure) {
            this.pressure = pressure;
            return this;
        }

        public Builder withArmId(final String armId) {
            this.armId = armId;
            return this;
        }

        public MergedData build() {
            notNull(timestamp);
            return new MergedData(this);
        }
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Double getSystolicBloodPressure() {
        return systolicBloodPressure;
    }

    public Double getSystolicBloodPressureUpperLimit() {
        return systolicBloodPressureUpperLimit;
    }

    public Double getSystolicBloodPressureLowerLimit() {
        return systolicBloodPressureLowerLimit;
    }

    public Double getDiastolicBloodPressure() {
        return diastolicBloodPressure;
    }

    public Double getDiastolicBloodPressureUpperLimit() {
        return diastolicBloodPressureUpperLimit;
    }

    public Double getDiastolicBloodPressureLowerLimit() {
        return diastolicBloodPressureLowerLimit;
    }

    public Double getHeartRate() {
        return heartRate;
    }

    public Double getHeartRateUpperLimit() {
        return heartRateUpperLimit;
    }

    public Double getHeartRateLowerLimit() {
        return heartRateLowerLimit;
    }

    public Double getOxygenSaturationLevel() {
        return oxygenSaturationLevel;
    }

    public Double getOxygenSaturationLevelUpperLimit() {
        return oxygenSaturationLevelUpperLimit;
    }

    public Double getOxygenSaturationLevelLowerLimit() {
        return oxygenSaturationLevelLowerLimit;
    }

    public Double getBloodLossRate() {
        return bloodLossRate;
    }

    public Integer getSystolicBloodPressureType() {
        return systolicBloodPressureType;
    }

    public Integer getDiastolicBloodPressureType() {
        return diastolicBloodPressureType;
    }

    public Integer getHeartRateType() {
        return heartRateType;
    }

    public Integer getOxygenSaturationLevelType() {
        return oxygenSaturationLevelType;
    }

    public Long getPatientId() {
        return patientId;
    }

    public String getSurgeryId() {
        return surgeryId;
    }

    public Double getRotation() {
        return rotation;
    }

    public Double getTemperature() {
        return temperature;
    }

    public Double getForce() {
        return force;
    }

    public Double getPressure() {
        return pressure;
    }

    public String getArmId() {
        return armId;
    }
}
