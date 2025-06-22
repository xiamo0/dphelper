package com.m01.dbhelper.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.m01.dbhelper.common.SqlSchedule;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Utility class for parsing JSON files into SqlSchedule objects
 */
public class JsonParser {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a JSON file into a SqlSchedule object
     *
     * @param filePath the path to the JSON file
     * @return the parsed SqlSchedule object
     * @throws IOException if an error occurs during parsing
     */
    public static SqlSchedule parseJsonFile(String filePath) throws IOException {
        return objectMapper.readValue(new File(filePath), SqlSchedule.class);
    }

    /**
     * Parses JSON content from an InputStream into a SqlSchedule object
     *
     * @param inputStream the InputStream containing JSON content
     * @return the parsed SqlSchedule object
     * @throws IOException if an error occurs during parsing
     */
    public static SqlSchedule parseJson(InputStream inputStream) throws IOException {
        return objectMapper.readValue(inputStream, SqlSchedule.class);
    }

    /**
     * Parses JSON string content into a SqlSchedule object
     *
     * @param jsonContent the JSON string to parse
     * @return the parsed SqlSchedule object
     * @throws IOException if an error occurs during parsing
     */
    public static SqlSchedule parseJsonString(String jsonContent) throws IOException {
        return objectMapper.readValue(jsonContent, SqlSchedule.class);
    }

    /**
     * Converts a SqlSchedule object to JSON string
     *
     * @param schedule the SqlSchedule object to convert
     * @return the JSON string representation
     * @throws IOException if an error occurs during conversion
     */
    public static String toJsonString(SqlSchedule schedule) throws IOException {
        return objectMapper.writeValueAsString(schedule);
    }

    /**
     * Writes a SqlSchedule object to a JSON file
     *
     * @param schedule the SqlSchedule object to write
     * @param filePath the path to write the JSON file
     * @throws IOException if an error occurs during writing
     */
    public static void writeJsonToFile(SqlSchedule schedule, String filePath) throws IOException {
        objectMapper.writeValue(new File(filePath), schedule);
    }
}
