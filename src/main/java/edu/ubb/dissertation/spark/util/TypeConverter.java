package edu.ubb.dissertation.spark.util;

import edu.ubb.dissertation.spark.model.PatientData;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.ubb.dissertation.spark.util.ModelCreatorHelper.createPatientData;

public final class TypeConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TypeConverter.class);

    private TypeConverter() {
    }

    public static PatientData mergePatientDataEntriesWithTheSameTimestamp(final Collection<PatientData> entries) {
        final List<String> abnormalVitalSigns = entries.stream()
                .map(PatientData::getAbnormalVitalSigns)
                .reduce(new ArrayList<>(), (a, b) -> Stream.concat(a.stream(), b.stream()).collect(Collectors.toList()));
        return createPatientData(entries.iterator().next(), abnormalVitalSigns);
    }

    private static Optional<LocalDateTime> convert(final String value, final String pattern) {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return Try.of(() -> LocalDateTime.parse(value, formatter))
                .onFailure(t -> LOGGER.error("Could not parse timestamp. StackTrace: {}", getStackTraceAsList(t)))
                .map(Optional::ofNullable)
                .getOrElseGet(t -> Optional.empty());
    }

    public static Optional<LocalDateTime> convert(final String value) {
        return convert(value, "yyyy-MM-ddTHH:mm:ss");
    }

    private static List<StackTraceElement> getStackTraceAsList(final Throwable throwable) {
        return Arrays.asList(throwable.getStackTrace());
    }
}
