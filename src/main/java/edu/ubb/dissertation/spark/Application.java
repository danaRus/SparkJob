package edu.ubb.dissertation.spark;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import edu.ubb.dissertation.spark.model.MergedData;
import edu.ubb.dissertation.spark.model.PatientData;
import edu.ubb.dissertation.spark.model.SensorData;
import edu.ubb.dissertation.spark.service.FileService;
import edu.ubb.dissertation.spark.util.TypeConverter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static edu.ubb.dissertation.spark.util.ModelCreatorHelper.createMergedData;
import static edu.ubb.dissertation.spark.util.FunctionHelper.*;
import static edu.ubb.dissertation.spark.util.TableHelper.*;


public class Application {

    private static FileService fileService = new FileService();

    public static void main(final String[] args) {
        // TODO add the reading of the interval from file
//        final LocalDateTime startTimestamp = fileService.readTimestampFromFile("");
//        final LocalDateTime endTimestamp = fileService.readTimestampFromFile("");
        final LocalDateTime startTimestamp = LocalDateTime.now().minusHours(7);
        final LocalDateTime endTimestamp = LocalDateTime.now();

        final String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        final SparkSession sparkSession = SparkSession.builder()
                .appName("BatchProcessingApp")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        // retrieve the data
        final Map<LocalDateTime, PatientData> patientData = retrievePatientData(sparkSession, startTimestamp, endTimestamp);
        final Map<LocalDateTime, SensorData> sensorData = retrieveSensorData(sparkSession, startTimestamp, endTimestamp);

        // configure the errorTypes
        patientData.values().forEach(PatientData::configureErrorTypes);

        // insert the values into the table with merged entries
        sparkSession.sql(String.format("CREATE TABLE IF NOT EXISTS merged_data (%s) USING hive", createMergedDataTableColumnsWithType()));

        // using this data, the table will contain the details needed to display what percentage of the surgery the
        // patient had the parameters outside of the ranges
        Dataset<MergedData> mergedData = sparkSession.createDataset(mergeData(patientData, sensorData), Encoders.bean(MergedData.class));
        mergedData.toDF(createMergedDataTableColumns()).write().mode("overwrite").saveAsTable("merged_data"); // use overwrite or append
    }

    private static Map<LocalDateTime, PatientData> retrievePatientData(final SparkSession sparkSession, final LocalDateTime startTimestamp,
                                                                       final LocalDateTime endTimestamp) {
        return mergeEntries(sparkSession.sql(String.format("SELECT %s FROM patient_data ORDER by timestamp", createPatientDataColumns()))
                .map(createPatientMapFunction(), Encoders.bean(PatientData.class)) // the data here is with a string for abnormal vital sign
                .filter(createPatientDataFilterFunction(startTimestamp, endTimestamp))
                .collectAsList())
                .stream()
                .collect(Collectors.toMap(PatientData::getTimestamp, patientData -> patientData));
    }

    private static Map<LocalDateTime, SensorData> retrieveSensorData(final SparkSession sparkSession, final LocalDateTime startTimestamp,
                                                                     final LocalDateTime endTimestamp) {
        return sparkSession.sql(String.format("SELECT %s FROM sensor_data ORDER by timestamp", createSensorDataColumns()))
                .map(createSensorMapFunction(), Encoders.bean(SensorData.class))
                .filter(createSensorDataFilterFunction(startTimestamp, endTimestamp))
                .collectAsList()
                .stream()
                .collect(Collectors.toMap(SensorData::getTimestamp, sensorData -> sensorData));
    }

    private static List<MergedData> mergeData(final Map<LocalDateTime, PatientData> patientData, final Map<LocalDateTime, SensorData> sensorData) {
        return patientData.keySet()
                .stream()
                .map(timestamp -> createMergedData(patientData.get(timestamp), sensorData.get(timestamp)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Process the entries that have the same timestamp in order to get the entire set of abnormal values in a single entry
     */
    private static List<PatientData> mergeEntries(final List<PatientData> patientDataEntries) {
        final Multimap<LocalDateTime, PatientData> entries = ArrayListMultimap.create();
        patientDataEntries.forEach(patientData -> entries.put(patientData.getTimestamp(), patientData));
        return entries.keySet()
                .stream()
                .map(entries::get)
                .map(TypeConverter::mergePatientDataEntriesWithTheSameTimestamp)
                .collect(Collectors.toList());
    }
}
