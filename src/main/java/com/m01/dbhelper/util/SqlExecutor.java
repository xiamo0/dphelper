package com.m01.dbhelper.util;

import com.m01.dbhelper.common.DbType;
import com.m01.dbhelper.common.SqlSchedule;
import com.m01.dbhelper.common.SqlTask;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.jsqlparser.JSQLParserException;

/**
 * Utility class for executing SQL schedules loaded from JSON files
 */
public class SqlExecutor {

    private static final Logger logger = Logger.getLogger(SqlExecutor.class.getName());

    /**
     * Executes SQL schedule from a JSON file
     *
     * @param jsonFilePath path to the JSON file containing the SQL schedule
     * @return true if execution completed successfully, false otherwise
     */
    public static boolean executeScheduleFromJson(String jsonFilePath) {
        try {
            // 初始化result.txt文件，以便在解析失败时也能记录错误
            ResultLogger.initResultFile();
            ResultLogger.logToResultFile("Starting to parse JSON file: " + jsonFilePath);

            SqlSchedule schedule = JsonParser.parseJsonFile(jsonFilePath);
            return executeSchedule(schedule);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to parse JSON file: " + jsonFilePath, e);
            // 将JSON解析失败的错误记录到result.txt文件
            ResultLogger.logToResultFile("ERROR: Failed to parse JSON file: " + jsonFilePath);
            ResultLogger.logToResultFile("Error message: " + e.getMessage());
            return false;
        }
    }

    /**
     * Executes the given SQL schedule
     *
     * @param schedule the SQL schedule to execute
     * @return true if execution completed successfully, false otherwise
     */
    public static boolean executeSchedule(SqlSchedule schedule) {
        logger.info("Executing SQL schedule: " + schedule.getScheduleName());
        ResultLogger.initResultFile(); // Initialize the result file

        List<SqlTask> tasks = schedule.getTaskList();
        if (tasks == null || tasks.isEmpty()) {
            logger.warning("No tasks found in the schedule");
            ResultLogger.logToResultFile("Schedule: " + schedule.getScheduleName() + " - No tasks found");
            return true;
        }

        // First phase: Validate all SQL statements before executing
        logger.info("Validating all SQL statements...");
        ResultLogger.logToResultFile("Schedule: " + schedule.getScheduleName() + " - Starting SQL validation");
        List<List<String>> validatedTaskSqlStatements = new ArrayList<>();

        for (SqlTask task : tasks) {
            List<String> validatedSqlStatements = validateTaskSql(task, schedule.getPolicyWhenError(), schedule.getDbType(), schedule.getScheduleName());

            // If validation fails and policy is "stop", return early without connecting to the database
            if (validatedSqlStatements == null && "stop".equalsIgnoreCase(schedule.getPolicyWhenError())) {
                logger.severe("Schedule execution stopped due to SQL validation failure and stop policy");
                ResultLogger.logToResultFile("Schedule: " + schedule.getScheduleName() + " - Execution stopped due to validation failures");
                return false;
            }

            if (validatedSqlStatements != null) {
                validatedTaskSqlStatements.add(validatedSqlStatements);
            }
        }

        logger.info("SQL validation completed. Connecting to database...");
        ResultLogger.logToResultFile("Schedule: " + schedule.getScheduleName() + " - SQL validation completed. Connecting to database...");

        // Second phase: Connect to the database and execute validated SQL
        Connection connection = null;
        try {
            connection = getConnection(schedule);

            for (int i = 0; i < tasks.size(); i++) {
                if (i >= validatedTaskSqlStatements.size() || validatedTaskSqlStatements.get(i) == null) {
                    continue; // Skip tasks that failed validation
                }

                SqlTask task = tasks.get(i);
                List<String> sqlStatements = validatedTaskSqlStatements.get(i);

                boolean taskResult = executeValidatedTask(task, connection, schedule.getPolicyWhenError(), sqlStatements, schedule.getScheduleName());
                if (!taskResult && "stop".equalsIgnoreCase(schedule.getPolicyWhenError())) {
                    logger.severe("Schedule execution stopped due to task execution failure and stop policy");
                    ResultLogger.logToResultFile("Schedule: " + schedule.getScheduleName() + " - Execution stopped due to task execution failure");
                    return false;
                }
            }

            ResultLogger.logToResultFile("Schedule: " + schedule.getScheduleName() + " - Execution completed successfully");
            return true;
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Database connection error", e);
            ResultLogger.logToResultFile("Schedule: " + schedule.getScheduleName() + " - Database connection error: " + e.getMessage());
            return false;
        } finally {
            closeConnection(connection);
        }
    }

    /**
     * Validates all SQL statements in a task
     *
     * @param task                    the SQL task to validate
     * @param schedulePolicyWhenError fallback error policy
     * @param scheduleDbType          fallback database type
     * @param scheduleName the name of the schedule
     * @return list of validated SQL statements, or null if validation failed and policy is "stop"
     */
    private static List<String> validateTaskSql(SqlTask task, String schedulePolicyWhenError,
                                              DbType scheduleDbType, String scheduleName) {
        logger.info("Validating SQL for task: " + task.getTaskName());
        ResultLogger.logToResultFile("Schedule: " + scheduleName + " - Task: " + task.getTaskName() + " - Starting validation");

        List<String> validatedSqlStatements = new ArrayList<>();

        // Get database type - either from task or use schedule's if task doesn't define one
        if (scheduleDbType == null) {
            String errorMsg = "Database type is not specified for task: " + task.getTaskName();
            logger.severe(errorMsg);
            ResultLogger.logToResultFile("Schedule: " + scheduleName + " - Task: " + task.getTaskName() + " - " + errorMsg);
            String policy = task.getPolicyWhenError() != null ? task.getPolicyWhenError() : schedulePolicyWhenError;
            return "stop".equalsIgnoreCase(policy) ? null : validatedSqlStatements;
        }

        // Process the SQL list - expanding file references
        for (String sqlEntry : task.getSqlList()) {
            try {
                if (sqlEntry.trim().toLowerCase().endsWith(".sql")) {
                    // This is a SQL file reference - read and validate its contents using SqlValidator
                    ResultLogger.logToResultFile("Schedule: " + scheduleName + " - Task: " + task.getTaskName() + " - Reading SQL file: " + sqlEntry.trim());
                    try {
                        List<String> sqlStatementsFromFile = SqlValidator.readSqlFile(sqlEntry.trim(), scheduleDbType);
                        for (String statement : sqlStatementsFromFile) {
                            if (!statement.trim().isEmpty()) {
                                // Validate SQL syntax
                                boolean isValid = SqlValidator.isValidSql(statement, scheduleDbType);
                                if (isValid) {
                                    ResultLogger.logToResultFile(scheduleName + "-" + task.getTaskName() + "-" + truncateSql(statement));
                                    ResultLogger.logToResultFile("parse success");
                                    validatedSqlStatements.add(statement);
                                } else {
                                    String errorMsg = "Invalid SQL syntax in file " + sqlEntry;
                                    logger.warning(errorMsg + ": " + statement);
                                    ResultLogger.logToResultFile(scheduleName + "-" + task.getTaskName() + "-" + truncateSql(statement));
                                    ResultLogger.logToResultFile("parse fail: Invalid SQL syntax");

                                    // Apply error policy for invalid SQL
                                    String policy = task.getPolicyWhenError() != null ?
                                        task.getPolicyWhenError() : schedulePolicyWhenError;
                                    if ("stop".equalsIgnoreCase(policy)) {
                                        return null;
                                    }
                                }
                            }
                        }
                    } catch (JSQLParserException e) {
                        String errorMsg = "Failed to parse SQL in file: " + sqlEntry;
                        logger.log(Level.SEVERE, errorMsg, e);
                        ResultLogger.logToResultFile(scheduleName + "-" + task.getTaskName() + "-" + sqlEntry);
                        ResultLogger.logToResultFile("parse fail: " + e.getMessage());

                        String policy = task.getPolicyWhenError() != null ?
                            task.getPolicyWhenError() : schedulePolicyWhenError;
                        if ("stop".equalsIgnoreCase(policy)) {
                            return null;
                        }
                    }
                } else {
                    // Direct SQL statement - validate it
                    String statement = sqlEntry.trim();
                    if (!statement.isEmpty()) {
                        boolean isValid = SqlValidator.isValidSql(statement, scheduleDbType);
                        if (isValid) {
                            ResultLogger.logToResultFile(scheduleName + "-" + task.getTaskName() + "-" + truncateSql(statement));
                            ResultLogger.logToResultFile("parse success");
                            validatedSqlStatements.add(statement);
                        } else {
                            String errorMsg = "Invalid SQL syntax: " + statement;
                            logger.warning(errorMsg);
                            ResultLogger.logToResultFile(scheduleName + "-" + task.getTaskName() + "-" + truncateSql(statement));
                            ResultLogger.logToResultFile("parse fail: Invalid SQL syntax");

                            // Apply error policy for invalid SQL
                            String policy = task.getPolicyWhenError() != null ?
                                task.getPolicyWhenError() : schedulePolicyWhenError;
                            if ("stop".equalsIgnoreCase(policy)) {
                                return null;
                            }
                        }
                    }
                }
            } catch (IOException e) {
                String errorMsg = "Failed to read SQL file: " + sqlEntry;
                logger.log(Level.SEVERE, errorMsg, e);
                ResultLogger.logToResultFile(scheduleName + "-" + task.getTaskName() + "-" + sqlEntry);
                ResultLogger.logToResultFile("parse fail: " + e.getMessage());

                String policy = task.getPolicyWhenError() != null ?
                    task.getPolicyWhenError() : schedulePolicyWhenError;

                if ("stop".equalsIgnoreCase(policy)) {
                    return null;
                }
            }
        }

        ResultLogger.logToResultFile("Schedule: " + scheduleName + " - Task: " + task.getTaskName() + " - Validation completed with " + validatedSqlStatements.size() + " valid statements");
        return validatedSqlStatements;
    }

    /**
     * Executes a task with pre-validated SQL statements
     *
     * @param task                    the SQL task to execute
     * @param connection              the database connection
     * @param schedulePolicyWhenError fallback error policy
     * @param sqlStatements           pre-validated SQL statements
     * @param scheduleName the name of the schedule
     * @return true if execution completed successfully, false otherwise
     */
    private static boolean executeValidatedTask(SqlTask task, Connection connection,
                                               String schedulePolicyWhenError, List<String> sqlStatements,
                                               String scheduleName) {
        logger.info("Executing SQL task: " + task.getTaskName());
        ResultLogger.logToResultFile("Schedule: " + scheduleName + " - Task: " + task.getTaskName() + " - Starting execution");

        String policy = task.getPolicyWhenError() != null ?
            task.getPolicyWhenError() : schedulePolicyWhenError;

        try {
            connection.setAutoCommit(false);

            for (String sql : sqlStatements) {
                try (Statement statement = connection.createStatement()) {
                    logger.fine("Executing SQL: " + sql);
                    ResultLogger.logToResultFile(scheduleName + "-" + task.getTaskName() + "-" + truncateSql(sql));
                    ResultLogger.logToResultFile("execute");

                    // Check if the SQL is a query (SELECT statement)
                    boolean isQuery = sql.trim().toLowerCase().startsWith("select");
                    boolean hasResults = false;

                    if (isQuery) {
                        try (ResultSet resultSet = statement.executeQuery(sql)) {
                            hasResults = true;
                            logQueryResults(resultSet, scheduleName, task.getTaskName(), sql);
                        }
                    } else {
                        // For non-query statements (INSERT, UPDATE, DELETE, etc.)
                        int rowsAffected = statement.executeUpdate(sql);
                        ResultLogger.logToResultFile("execution success - rows affected: " + rowsAffected);
                    }

                    if (!hasResults) {
                        ResultLogger.logToResultFile("execution success");
                    }
                } catch (SQLException e) {
                    String errorMsg = "SQL execution error: " + sql;
                    logger.log(Level.SEVERE, errorMsg, e);
                    ResultLogger.logToResultFile(scheduleName + "-" + task.getTaskName() + "-" + truncateSql(sql));
                    ResultLogger.logToResultFile("execution fail: " + e.getMessage());

                    if ("stop".equalsIgnoreCase(policy)) {
                        connection.rollback();
                        return false;
                    }
                }
            }

            connection.commit();
            ResultLogger.logToResultFile("Schedule: " + scheduleName + " - Task: " + task.getTaskName() + " - Execution completed successfully");
            return true;
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Transaction error", e);
            ResultLogger.logToResultFile("Schedule: " + scheduleName + " - Task: " + task.getTaskName() + " - Transaction error: " + e.getMessage());
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                logger.log(Level.SEVERE, "Rollback error", rollbackEx);
                ResultLogger.logToResultFile("Schedule: " + scheduleName + " - Task: " + task.getTaskName() + " - Rollback error: " + rollbackEx.getMessage());
            }
            return false;
        }
    }

    /**
     * Truncate SQL statement for readable logging
     *
     * @param sql the SQL statement to truncate
     * @return truncated SQL statement
     */
    private static String truncateSql(String sql) {
        return ResultLogger.truncateSql(sql);
    }

    /**
     * Logs the results of a SQL query to the result file
     *
     * @param resultSet  the ResultSet object containing the query results
     * @param scheduleName the name of the schedule
     * @param taskName   the name of the task
     * @param sql        the SQL query that was executed
     * @throws SQLException if an error occurs while processing the ResultSet
     */
    private static void logQueryResults(ResultSet resultSet, String scheduleName, String taskName, String sql) throws SQLException {
        ResultLogger.logQueryResults(resultSet, scheduleName, taskName, sql);
    }
}
