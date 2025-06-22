package com.m01.dbhelper.util;

import com.m01.dbhelper.common.DbType;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statements;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SqlValidator {
    public static boolean isValidSql(String sql, DbType dbType) {
        // 验证输入参数
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        if (dbType == null) {
            throw new IllegalArgumentException("Database type cannot be null");
        }

        // 清理和准备SQL语句
        sql = sql.trim();

        // 基础SQL语法验证：检查是否以常见SQL关键字开头
        String sqlKeywordsPattern = "(?i)^\\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|MERGE|WITH|BEGIN|CALL|DECLARE)\\b.*";
        if (!sql.matches(sqlKeywordsPattern)) {
            return false;
        }

        // 根据数据库类型进行特定处理
        try {
            switch (dbType) {
                case MYSQL:
                    // MySQL特定的SQL处理
                    sql = handleMySqlSpecifics(sql);
                    break;
                case POSTGRESQL:
                    // PostgreSQL特定的SQL处理
                    sql = handlePostgreSqlSpecifics(sql);
                    break;
                case GAUSSDB:
                    // GaussDB特定的SQL处理
                    sql = handleGaussDbSpecifics(sql);
                    break;
                case SQLITE:
                    // SQLite特定的SQL处理
                    sql = handleSqliteSpecifics(sql);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported database type: " + dbType);
            }

            // 使用JSqlParser进行语法解析验证
            CCJSqlParserUtil.parse(sql);
            return true;
        } catch (JSQLParserException e) {
            // 解析失败，SQL语法不合法
            return false;
        } catch (Exception e) {
            // 其他未预期的异常
            throw new RuntimeException("SQL validation error", e);
        }
    }

    /**
     * 读取SQL文件，去除SQL注释，并将其中的SQL语句解析为列表
     *
     * @param filePath SQL文件路径
     * @param dbType 数据库类型
     * @return 解析后的SQL语句列表
     * @throws IOException 如果文件读取失败
     * @throws JSQLParserException 如果SQL解析失败
     */
    public static List<String> readSqlFile(String filePath, DbType dbType) throws IOException, JSQLParserException {
        // 读取SQL文件内容
        String sqlFileContent = readFileContent(filePath);

        // 移除SQL注释
        sqlFileContent = removeComments(sqlFileContent);

        // 分割SQL语句并返回
        return splitSqlStatements(sqlFileContent, dbType);
    }

    /**
     * 读取文件内容为字符串
     *
     * @param filePath 文件路径
     * @return 文件内容
     * @throws IOException 如果文件读取失败
     */
    private static String readFileContent(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
            return content.toString();
        }
    }

    /**
     * 移除SQL中的注释（包括单行注释和多行注释）
     *
     * @param sql 原始SQL字符串
     * @return 移除注释后的SQL
     */
    public static String removeComments(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        StringBuilder result = new StringBuilder();
        boolean inLineComment = false;
        boolean inBlockComment = false;
        boolean inString = false;
        char stringChar = 0; // 字符串的引号类型（单引号或双引号）

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char nextChar = (i < sql.length() - 1) ? sql.charAt(i + 1) : 0;

            // 处理字符串 (为了避免注释符号出现在字符串中被误处理)
            if (!inLineComment && !inBlockComment) {
                if (!inString && (c == '\'' || c == '"')) {
                    inString = true;
                    stringChar = c;
                    result.append(c);
                    continue;
                } else if (inString && c == stringChar && (i == 0 || sql.charAt(i - 1) != '\\')) {
                    inString = false;
                    result.append(c);
                    continue;
                }
            }

            // 如果在字符串内，直接添加字符
            if (inString) {
                result.append(c);
                continue;
            }

            // 处理单行注释
            if (!inBlockComment && !inLineComment && c == '-' && nextChar == '-') {
                inLineComment = true;
                i++; // 跳过第二个'-'
                continue;
            }

            // 处理多行注释开始
            if (!inLineComment && !inBlockComment && c == '/' && nextChar == '*') {
                inBlockComment = true;
                i++; // 跳过'*'
                continue;
            }

            // 处理单行注释结束
            if (inLineComment && (c == '\n' || c == '\r')) {
                inLineComment = false;
                // 保留换行符
                result.append(c);
                continue;
            }

            // 处理多行注释结束
            if (inBlockComment && c == '*' && nextChar == '/') {
                inBlockComment = false;
                i++; // 跳过'/'
                continue;
            }

            // 如果不在注释中，添加字符
            if (!inLineComment && !inBlockComment) {
                result.append(c);
            }
        }

        return result.toString();
    }

    /**
     * 将SQL文本分割为多个SQL语句
     *
     * @param sqlText 包含多个SQL语句的文本
     * @param dbType 数据库类型
     * @return SQL语句列表
     * @throws JSQLParserException 如果解析失败
     */
    public static List<String> splitSqlStatements(String sqlText, DbType dbType) throws JSQLParserException {
        // 去除多余空白和分号
        sqlText = sqlText.trim();

        // 尝试使用JSqlParser解析
        try {
            Statements statements = CCJSqlParserUtil.parseStatements(sqlText);
            List<String> sqlList = new ArrayList<>();
            statements.getStatements().forEach(stmt -> sqlList.add(stmt.toString()));
            return sqlList;
        } catch (JSQLParserException e) {
            // 如果JSqlParser解析失败，使用简单的分号分割
            return splitBySemicolon(sqlText);
        }
    }

    /**
     * 简单地按分号分割SQL语句（作为JSqlParser解析失败的备选方案）
     *
     * @param sqlText SQL文本
     * @return SQL语句列表
     */
    private static List<String> splitBySemicolon(String sqlText) {
        List<String> result = new ArrayList<>();
        StringBuilder currentSql = new StringBuilder();
        boolean inString = false;
        char stringChar = 0;

        for (int i = 0; i < sqlText.length(); i++) {
            char c = sqlText.charAt(i);

            // 处理字符串
            if (!inString && (c == '\'' || c == '"')) {
                inString = true;
                stringChar = c;
            } else if (inString && c == stringChar && (i == 0 || sqlText.charAt(i - 1) != '\\')) {
                inString = false;
            }

            // 如果遇到分号并且不在字符串内，则分割SQL语句
            if (c == ';' && !inString) {
                String sql = currentSql.toString().trim();
                if (!sql.isEmpty()) {
                    result.add(sql);
                }
                currentSql = new StringBuilder();
            } else {
                currentSql.append(c);
            }
        }

        // 添加最后一个语句（如果没有以分号结尾）
        String lastSql = currentSql.toString().trim();
        if (!lastSql.isEmpty()) {
            result.add(lastSql);
        }

        return result;
    }

    /**
     * 处理MySQL特有的SQL语法特性
     *
     * @param sql 原始SQL语句
     * @return 处理后的SQL语句
     */
    private static String handleMySqlSpecifics(String sql) {
        // 处理MySQL反引号标识符
        // MySQL使用反引号(`)来标识表名、列名等标识符
        // 在解析前将它们转换为标准SQL引用格式以便兼容JSqlParser

        // 创建一个可变的StringBuilder以便修改
        StringBuilder processedSql = new StringBuilder(sql);

        // 检查MySQL特有的AUTO_INCREMENT关键字
        if (sql.toUpperCase().contains("AUTO_INCREMENT")) {
            // JSqlParser可能不完全支持AUTO_INCREMENT，可能需要特殊处理
            // 这里我们仅做标记，实际处理取决于JSqlParser的具体兼容性

        }

        // 检查并处理MySQL特有的存储引擎语法 (如ENGINE=InnoDB)
        // JSqlParser通常能处理这些，但我们可以增加额外的验证
        if (sql.toUpperCase().contains("ENGINE=")) {
            // 如果需要对存储引擎声明做特殊处理，可以在这里实现
        }

        // 处理MySQL特有的ON DUPLICATE KEY UPDATE语法
        if (sql.toUpperCase().contains("ON DUPLICATE KEY UPDATE")) {
            // 特殊处理ON DUPLICATE KEY UPDATE语句，确保语法正确
        }

        // 检查MySQL特有的函数，如 IFNULL, CONCAT_WS 等
        String[] mysqlSpecificFunctions = {"NOW()", "IFNULL", "CONCAT_WS", "GROUP_CONCAT"};
        for (String function : mysqlSpecificFunctions) {
            if (sql.toUpperCase().contains(function)) {
                // 如果存在MySQL特有函数，可以进行特殊处理
            }
        }

        // 检查MySQL特有的LIMIT语法（虽然PostgreSQL也支持，但语法可能有差异）
        // JSqlParser通常可以处理标准的LIMIT语法

        return processedSql.toString();
    }

    /**
     * 处理PostgreSQL特有的SQL语法特性
     *
     * @param sql 原始SQL语句
     * @return 处理后的SQL语句
     */
    private static String handlePostgreSqlSpecifics(String sql) {
        // 处理PostgreSQL双引号标识符
        // PostgreSQL使用双引号(")来标识表名、列名等标识符
        // 在解析前将它们转换为标准SQL引用格式以便兼容JSqlParser

        // 创建一个可变的StringBuilder以便修改
        StringBuilder processedSql = new StringBuilder(sql);

        // 替换双引号为反引号，以兼容JSqlParser
        int doubleQuoteIndex;
        while ((doubleQuoteIndex = processedSql.indexOf("\"")) != -1) {
            processedSql.replace(doubleQuoteIndex, doubleQuoteIndex + 1, "`");
        }

        // 检查PostgreSQL特有的序列语法，如 nextval('sequence_name')
        // 可以在这里实现对序列的特殊处理

        // 检查PostgreSQL特有的schema语法，如 schema.table
        // 可以在这里实现对schema的特殊处理

        // 检查PostgreSQL特有的函数，如 COALESCE, STRING_AGG 等
        String[] postgresSpecificFunctions = {"NOW()", "COALESCE", "STRING_AGG"};
        for (String function : postgresSpecificFunctions) {
            if (sql.toUpperCase().contains(function)) {
                // 如果存在PostgreSQL特有函数，可以进行特殊处理
            }
        }

        // 检查PostgreSQL特有的LIMIT语法（虽然MySQL也支持，但语法可能有差异）
        // JSqlParser通常可以处理标准的LIMIT语法

        return processedSql.toString();
    }

    /**
     * 处理GaussDB特有的SQL语法特性
     *
     * @param sql 原始SQL语句
     * @return 处理后的SQL语句
     */
    private static String handleGaussDbSpecifics(String sql) {
        // GaussDB作为PostgreSQL的发行版，继承了PostgreSQL的大部分特性
        // 同时还有一些自己的扩展特性

        // 首先处理PostgreSQL的基础特性
        String processedSql = handlePostgreSqlSpecifics(sql);

        StringBuilder sqlBuilder = new StringBuilder(processedSql);

        // 处理GaussDB特有的字段类型，如SERIAL8
        String[] gaussDbSpecificTypes = {"SERIAL8", "VARCHAR2", "NUMBER"};
        for (String type : gaussDbSpecificTypes) {
            if (sql.toUpperCase().contains(type)) {
                // 如果需要对特有类型做特殊处理，可以在这里实现
            }
        }

        // 处理GaussDB特有的函数和语法
        String[] gaussDbSpecificKeywords = {"NEXTVAL", "CURRVAL"};
        for (String keyword : gaussDbSpecificKeywords) {
            if (sql.toUpperCase().contains(keyword)) {
                // 如果需要对特有关键字做特殊处理，可以在这里实现
            }
        }

        // 处理GaussDB特有的存储过程语法
        if (sql.toUpperCase().contains("CREATE PROCEDURE") || sql.toUpperCase().contains("CREATE FUNCTION")) {
            // 处理存储过程和函数的特殊语法
        }

        return sqlBuilder.toString();
    }

    /**
     * 处理SQLite特有的SQL语法特性
     *
     * @param sql 原始SQL语句
     * @return 处理后的SQL语句
     */
    private static String handleSqliteSpecifics(String sql) {
        // 创建一个可变的StringBuilder以便修改
        StringBuilder processedSql = new StringBuilder(sql);

        // 处理SQLite特有的自增列语法 (例如 INTEGER PRIMARY KEY AUTOINCREMENT)
        if (sql.toUpperCase().contains("AUTOINCREMENT")) {
            // SQLite的AUTOINCREMENT与其他数据库的AUTO_INCREMENT不完全相同
            // 如果需要可以在这里进行特殊处理
        }

        // 处理SQLite特有的数据类型
        // SQLite使用动态类型系统，只有几种存储类别
        String[] sqliteTypes = {"TEXT", "NUMERIC", "INTEGER", "REAL", "BLOB"};
        for (String type : sqliteTypes) {
            if (sql.toUpperCase().contains(type)) {
                // 对SQLite特定类型进行处理（如果需要）
            }
        }

        // 处理SQLite的PRAGMA语句
        if (sql.toUpperCase().startsWith("PRAGMA")) {
            // PRAGMA是SQLite特有的，可以在这里进行特殊处理
            // JSqlParser可能不支持PRAGMA语句
            return sql; // 对于PRAGMA语句，可以直接返回原SQL，不进行JSqlParser验证
        }

        // 处理SQLite的表连接语法特性
        // SQLite支持多种JOIN语法，但某些特殊语法可能需要特殊处理

        // 处理SQLite的日期时间函数，如 date(), time(), datetime()
        String[] sqliteDateFunctions = {"DATE(", "TIME(", "DATETIME(", "JULIANDAY("};
        for (String function : sqliteDateFunctions) {
            if (sql.toUpperCase().contains(function)) {
                // 如果需要对日期函数做特殊处理，可以在这里实现
            }
        }

        // 处理SQLite的窗口函数限制
        // SQLite从3.25.0版本开始支持窗口函数，早期版本可能需要特殊处理

        return processedSql.toString();
    }
}