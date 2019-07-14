package edu.ubb.dissertation.spark.model;

import java.time.LocalDateTime;

import static org.apache.commons.lang3.Validate.notNull;

public class SensorData {

    private LocalDateTime timestamp;
    private Double rotation;
    private Double temperature;
    private Double force;
    private Double pressure;
    private String armId;

    public SensorData() {
    }

    private SensorData(final Builder builder) {
        this.timestamp = builder.timestamp;
        this.rotation = builder.rotation;
        this.temperature = builder.temperature;
        this.force = builder.force;
        this.pressure = builder.pressure;
        this.armId = builder.armId;
    }

    public static class Builder {

        private LocalDateTime timestamp;
        private Double rotation;
        private Double temperature;
        private Double force;
        private Double pressure;
        private String armId;

        public Builder withTimestamp(final LocalDateTime timestamp) {
            this.timestamp = timestamp;
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

        public SensorData build() {
            notNull(timestamp);
            return new SensorData(this);
        }
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
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
