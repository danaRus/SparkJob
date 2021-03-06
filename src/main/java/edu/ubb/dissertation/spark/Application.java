package edu.ubb.dissertation.spark;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import edu.ubb.dissertation.spark.exception.ConnectionException;
import edu.ubb.dissertation.spark.model.MergedData;
import edu.ubb.dissertation.spark.model.PatientData;
import edu.ubb.dissertation.spark.model.SensorData;
import edu.ubb.dissertation.spark.service.FileService;
import edu.ubb.dissertation.spark.util.ModelCreatorHelper;
import edu.ubb.dissertation.spark.util.TypeConverter;
import io.vavr.control.Try;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static edu.ubb.dissertation.spark.util.ModelCreatorHelper.createMergedData;
import static edu.ubb.dissertation.spark.util.TableHelper.*;


public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private static final String CONNECTION_URL = "jdbc:hive2://localhost:10000/";
    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String THRIFT_URL = "thrift://localhost:9083";

    private static FileService fileService = new FileService();

    public static void main(final String[] args) {
        // TODO add the reading of the interval from file
//        final LocalDateTime startTimestamp = fileService.readTimestampFromFile("");
//        final LocalDateTime endTimestamp = fileService.readTimestampFromFile("");
        Try.run(() -> Class.forName(DRIVER_NAME))
                .onFailure(t -> LOGGER.error("Could not get driver. Message: {}", t.getMessage()))
                .getOrElseThrow(ConnectionException::create);
        final LocalDateTime startTimestamp = LocalDateTime.now().minusHours(7);
        final LocalDateTime endTimestamp = LocalDateTime.now();

        final SparkConf sparkConf = new SparkConf().setMaster("local").set("url", CONNECTION_URL);
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        final HiveContext hiveContext = new HiveContext(javaSparkContext.sc());
        hiveContext.setConf("hive.metastore.uris", THRIFT_URL);

        // retrieve the data
        final Map<LocalDateTime, PatientData> patientData = retrievePatientData(hiveContext, startTimestamp, endTimestamp);
        final Map<LocalDateTime, SensorData> sensorData = retrieveSensorData(hiveContext, startTimestamp, endTimestamp);

        // configure the errorTypes
        patientData.values().forEach(PatientData::configureErrorTypes);

        // insert the values into the table with merged entries
        hiveContext.sql(String.format("CREATE TABLE IF NOT EXISTS dissertation.merged_data (%s)", createMergedDataTableColumnsWithType()));

        // using this data, the table will contain the details needed to display what percentage of the surgery the
        // patient had the parameters outside of the ranges

        final Dataset<MergedData> mergedData = hiveContext.createDataset(mergeData(patientData, sensorData), Encoders.bean(MergedData.class));
        mergedData.toDF().write().mode("overwrite").saveAsTable("dissertation.merged_data"); // use overwrite or append
    }

    private static Map<LocalDateTime, PatientData> retrievePatientData(final HiveContext hiveContext, final LocalDateTime startTimestamp,
                                                                       final LocalDateTime endTimestamp) {
        return mergeEntries(hiveContext.sql(String.format("SELECT %s FROM dissertation.patient_data ORDER by timestamp", createPatientDataColumns()))
                .collectAsList()
                .stream()
                .map(ModelCreatorHelper::createPatientDataFromRow)
                .collect(Collectors.toList()))
                .stream()
                .filter(patientData -> isInTimeRange(patientData.getTimestamp(), startTimestamp, endTimestamp))
                .collect(Collectors.toMap(PatientData::getTimestamp, patientData -> patientData));
    }

    private static Map<LocalDateTime, SensorData> retrieveSensorData(final HiveContext hiveContext, final LocalDateTime startTimestamp,
                                                                     final LocalDateTime endTimestamp) {
        return hiveContext.sql(String.format("SELECT %s FROM dissertation.sensor_data ORDER by timestamp", createSensorDataColumns()))
                .collectAsList()
                .stream()
                .map(ModelCreatorHelper::createSensorDataFromRow)
                .filter(sensorData -> isInTimeRange(sensorData.getTimestamp(), startTimestamp, endTimestamp))
                .collect(Collectors.toMap(SensorData::getTimestamp, sensorData -> sensorData));
    }

    private static boolean isInTimeRange(final LocalDateTime timestamp, final LocalDateTime startTimestamp,
                                         final LocalDateTime endTimestamp) {
        return timestamp.isAfter(startTimestamp) && timestamp.isBefore(endTimestamp);
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
