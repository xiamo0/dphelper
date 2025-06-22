package com.m01.dbhelper.util;

import com.m01.dbhelper.common.SqlSchedule;
import com.m01.dbhelper.common.SqlTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsonParserTest {

    @TempDir
    Path tempDir;

    private String createSampleJson() {
        return "{\n" +
                "  \"scheduleName\": \"Test Schedule\",\n" +
                "  \"policyWhenError\": \"continue\",\n" +
                "  \"dbType\": \"mysql\",\n" +
                "  \"dbUrl\": \"jdbc:mysql://localhost:3306/testdb\",\n" +
                "  \"dbUser\": \"testuser\",\n" +
                "  \"dbPassword\": \"testpass\",\n" +
                "  \"taskList\": [\n" +
                "    {\n" +
                "      \"taskName\": \"Create Tables\",\n" +
                "      \"policyWhenError\": \"stop\",\n" +
                "      \"sqlList\": [\n" +
                "        \"CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name VARCHAR(100))\",\n" +
                "        \"CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))\"\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"taskName\": \"Insert Data\",\n" +
                "      \"policyWhenError\": \"continue\",\n" +
                "      \"sqlList\": [\n" +
                "        \"INSERT INTO users VALUES (1, 'John Doe')\",\n" +
                "        \"INSERT INTO users VALUES (2, 'Jane Smith')\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";
    }

    private void assertValidSchedule(SqlSchedule schedule) {
        assertEquals("Test Schedule", schedule.getScheduleName());
        assertEquals("continue", schedule.getPolicyWhenError());
        assertEquals("mysql", schedule.getDbType());
        assertEquals("jdbc:mysql://localhost:3306/testdb", schedule.getDbUrl());
        assertEquals("testuser", schedule.getDbUser());
        assertEquals("testpass", schedule.getDbPassword());

        List<SqlTask> tasks = schedule.getTaskList();
        assertNotNull(tasks);
        assertEquals(2, tasks.size());

        // Check the first task
        SqlTask task1 = tasks.get(0);
        assertEquals("Create Tables", task1.getTaskName());
        assertEquals("stop", task1.getPolicyWhenError());
        assertEquals(2, task1.getSqlList().size());
        assertEquals("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name VARCHAR(100))", task1.getSqlList().get(0));

        // Check the second task
        SqlTask task2 = tasks.get(1);
        assertEquals("Insert Data", task2.getTaskName());
        assertEquals("continue", task2.getPolicyWhenError());
        assertEquals(2, task2.getSqlList().size());
    }

    @Test
    void testParseJsonString() throws IOException {
        String jsonContent = createSampleJson();

        SqlSchedule schedule = JsonParser.parseJsonString(jsonContent);

        assertValidSchedule(schedule);
    }

    @Test
    void testParseJsonFile() throws IOException {
        String jsonContent = createSampleJson();
        Path jsonFile = tempDir.resolve("test-schedule.json");
        Files.write(jsonFile, jsonContent.getBytes(StandardCharsets.UTF_8));

        SqlSchedule schedule = JsonParser.parseJsonFile(jsonFile.toString());

        assertValidSchedule(schedule);
    }

    @Test
    void testParseJsonInputStream() throws IOException {
        String jsonContent = createSampleJson();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(
            jsonContent.getBytes(StandardCharsets.UTF_8)
        );

        SqlSchedule schedule = JsonParser.parseJson(inputStream);

        assertValidSchedule(schedule);
    }

    @Test
    void testToJsonString() throws IOException {
        // Create a schedule object
        SqlSchedule schedule = new SqlSchedule();
        schedule.setScheduleName("Test Schedule");
        schedule.setPolicyWhenError("continue");
        schedule.setDbType("mysql");
        schedule.setDbUrl("jdbc:mysql://localhost:3306/testdb");
        schedule.setDbUser("testuser");
        schedule.setDbPassword("testpass");

        SqlTask task1 = new SqlTask();
        task1.setTaskName("Sample Task");
        task1.setPolicyWhenError("stop");
        task1.setSqlList(Arrays.asList("SELECT * FROM users", "UPDATE users SET name='Updated'"));

        schedule.setTaskList(Arrays.asList(task1));

        // Convert to JSON
        String jsonString = JsonParser.toJsonString(schedule);

        // Parse back and verify
        SqlSchedule parsedSchedule = JsonParser.parseJsonString(jsonString);

        assertEquals(schedule.getScheduleName(), parsedSchedule.getScheduleName());
        assertEquals(schedule.getDbType(), parsedSchedule.getDbType());
        assertEquals(1, parsedSchedule.getTaskList().size());
        assertEquals("Sample Task", parsedSchedule.getTaskList().get(0).getTaskName());
    }

    @Test
    void testWriteJsonToFile() throws IOException {
        // Create a schedule object
        SqlSchedule schedule = new SqlSchedule();
        schedule.setScheduleName("Test Schedule");
        schedule.setPolicyWhenError("continue");
        schedule.setDbType("mysql");
        schedule.setDbUrl("jdbc:mysql://localhost:3306/testdb");
        schedule.setDbUser("testuser");
        schedule.setDbPassword("testpass");

        SqlTask task1 = new SqlTask();
        task1.setTaskName("Sample Task");
        task1.setPolicyWhenError("stop");
        task1.setSqlList(Arrays.asList("SELECT * FROM users", "UPDATE users SET name='Updated'"));

        schedule.setTaskList(Arrays.asList(task1));

        // Write to file
        Path outputFile = tempDir.resolve("output-schedule.json");
        JsonParser.writeJsonToFile(schedule, outputFile.toString());

        // Verify file exists
        assertTrue(Files.exists(outputFile));

        // Read back and verify
        SqlSchedule parsedSchedule = JsonParser.parseJsonFile(outputFile.toString());

        assertEquals(schedule.getScheduleName(), parsedSchedule.getScheduleName());
        assertEquals(schedule.getDbType(), parsedSchedule.getDbType());
        assertEquals(1, parsedSchedule.getTaskList().size());
        assertEquals("Sample Task", parsedSchedule.getTaskList().get(0).getTaskName());
    }

    @Test
    void testInvalidJson() {
        String invalidJson = "{\"scheduleName\": \"Invalid JSON";

        assertThrows(IOException.class, () -> {
            JsonParser.parseJsonString(invalidJson);
        });
    }
}
