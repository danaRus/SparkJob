package edu.ubb.dissertation.spark.service;

import edu.ubb.dissertation.spark.util.TypeConverter;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

public class FileService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileService.class);

    private static final LocalDateTime FALLBACK_TIMESTAMP = LocalDateTime.of(2019, 7, 1, 0, 0, 0);

    public LocalDateTime readTimestampFromFile(final String fileName) {
        return createPath(fileName)
                .map(Paths::get)
                .flatMap(this::readContent)
                .flatMap(TypeConverter::convert)
                .orElse(FALLBACK_TIMESTAMP);
    }

//    public void writeTimestampToFile(final LocalDateTime timestamp, final String fileName) {
//        createPath(fileName)
//                .map(Paths::get)
//                .ifPresent(path -> writeContent(path, timestamp.toString().getBytes()));
//    }

    private Optional<URI> createPath(final String fileName) {
        return Try.of(() -> Objects.requireNonNull(getClass().getClassLoader().getResource(fileName)).toURI())
                .onFailure(t -> LOGGER.error("Could not get file URI. Message: {}", t.getMessage()))
                .map(Optional::ofNullable)
                .getOrElseGet(t -> Optional.empty());
    }

    private Optional<String> readContent(final Path path) {
        return Try.of(() -> Files.lines(path))
                .onFailure(t -> LOGGER.error("Could not read content from path: {}. Message: {}", path.getFileName(), t.getMessage()))
                .map(lines -> lines.reduce((a, b) -> b))
                .getOrElseGet(t -> Optional.empty());
    }

//    private void writeContent(final Path path, final byte[] content) {
//        Try.run(() -> Files.write(path, content))
//                .onFailure(t -> LOGGER.error("Could not write content to path: {}. Message: {}", path.getFileName(), t.getMessage()));
//    }
}
