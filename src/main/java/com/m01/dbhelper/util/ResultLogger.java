package com.m01.dbhelper.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 用于记录结果到文件的工具类
 */
public class ResultLogger {
    private static final Logger logger = Logger.getLogger(ResultLogger.class.getName());
    private static final String DEFAULT_RESULT_FILE = "result.txt";
    private static String resultFile = DEFAULT_RESULT_FILE;

    /**
     * 设置结果文件路径
     *
     * @param filePath 文件路径
     */
    public static void setResultFile(String filePath) {
        resultFile = filePath;
    }

    /**
     * 获取当前结果文件路径
     *
     * @return 当前结果文件路径
     */
    public static String getResultFile() {
        return resultFile;
    }

    /**
     * 初始化结果文件，添加头部信息
     */
    public static void initResultFile() {
        try {
            File file = new File(resultFile);
            // 如果文件存在则追加内容，否则创建新文件
            boolean append = file.exists();

            try (PrintWriter writer = new PrintWriter(new FileWriter(file, append))) {
                if (!append) {
                    writer.println("SQL Execution Results - " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                    writer.println("===============================================================");
                } else {
                    writer.println("\n\n===============================================================");
                    writer.println("New Execution - " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                    writer.println("===============================================================");
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to initialize result file: " + resultFile, e);
        }
    }

    /**
     * 记录消息到结果文件
     *
     * @param message 要记录的消息
     */
    public static void logToResultFile(String message) {
        try {
            Files.write(Paths.get(resultFile), (message + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to write to result file: " + resultFile, e);
        }
    }

    /**
     * 截断 SQL 语句以便于日志记录
     *
     * @param sql SQL 语句
     * @return 截断后的 SQL 语句
     */
    public static String truncateSql(String sql) {
        // 限制 SQL 长度以提高日志可读性
        final int MAX_LENGTH = 100;
        if (sql.length() > MAX_LENGTH) {
            return sql.substring(0, MAX_LENGTH) + "...";
        }
        return sql;
    }

    /**
     * 记录 SQL 查询结果到结果文件
     *
     * @param resultSet    包含查询结果的 ResultSet 对象
     * @param scheduleName 调度名称
     * @param taskName     任务名称
     * @param sql          执行的 SQL 查询
     * @throws SQLException 处理 ResultSet 时出错
     */
    public static void logQueryResults(ResultSet resultSet, String scheduleName, String taskName, String sql) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 记录列名
        StringBuilder columnNames = new StringBuilder("Query Results - Columns: ");
        for (int i = 1; i <= columnCount; i++) {
            columnNames.append(metaData.getColumnName(i));
            if (i < columnCount) {
                columnNames.append(", ");
            }
        }
        logToResultFile(columnNames.toString());

        // 记录行数据
        int rowCount = 0;
        while (resultSet.next()) {
            rowCount++;
            StringBuilder row = new StringBuilder("Row " + rowCount + ": ");
            for (int i = 1; i <= columnCount; i++) {
                row.append(resultSet.getString(i));
                if (i < columnCount) {
                    row.append(", ");
                }
            }
            logToResultFile(row.toString());
        }

        logToResultFile("Total rows: " + rowCount);
    }
}
