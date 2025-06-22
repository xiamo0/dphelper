package com.m01.dbhelper.util;

import com.m01.dbhelper.common.DbType;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

class SqlValidatorTest {

    private static Path tempSqlFile;

    @BeforeAll
    static void setUp() throws IOException {
        // 创建临时SQL文件用于测试
        tempSqlFile = Files.createTempFile("test_sql_", ".sql");
        String sqlContent = "-- This is a comment\n" +
                "SELECT * FROM users; /* This is a block comment */\n" +
                "-- Another comment\n" +
                "INSERT INTO products (name, price) VALUES ('Product 1', 10.99);\n" +
                "/* Multi-line\n" +
                "   block comment */\n" +
                "UPDATE orders SET status = 'shipped' WHERE id = 1;";
        Files.write(tempSqlFile, sqlContent.getBytes(), StandardOpenOption.WRITE);
    }

    @Test
    @DisplayName("测试有效SQL语句")
    void testValidSql() {
        String validSql = "SELECT * FROM users WHERE age > 18";
        Assertions.assertTrue(SqlValidator.isValidSql(validSql, DbType.MYSQL));
        Assertions.assertTrue(SqlValidator.isValidSql(validSql, DbType.POSTGRESQL));
        Assertions.assertTrue(SqlValidator.isValidSql(validSql, DbType.GAUSSDB));
        Assertions.assertTrue(SqlValidator.isValidSql(validSql, DbType.SQLITE));
    }

    @Test
    @DisplayName("测试无效SQL语句")
    void testInvalidSql() {
        // 空SQL
        Assertions.assertFalse(SqlValidator.isValidSql("", DbType.MYSQL));
        Assertions.assertFalse(SqlValidator.isValidSql(null, DbType.MYSQL));

        // 语法错误的SQL
        String invalidSql = "SELECT FROM users WHERE;";
        Assertions.assertFalse(SqlValidator.isValidSql(invalidSql, DbType.MYSQL));

        // 非SQL语句
        String notSql = "This is not a SQL statement";
        Assertions.assertFalse(SqlValidator.isValidSql(notSql, DbType.MYSQL));
    }

    @Test
    @DisplayName("测试DbType不能为null")
    void testNullDbType() {
        String sql = "SELECT * FROM users";
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            SqlValidator.isValidSql(sql, null);
        });
    }

    @Test
    @DisplayName("测试移除SQL注释")
    void testRemoveComments() {
        String sqlWithComments = "SELECT * FROM users; -- This is a comment\n" +
                "/* This is a block comment */\n" +
                "INSERT INTO table (col1, col2) VALUES ('val1', 'val2'); -- Another comment";

        String expectedSql = "SELECT * FROM users; \n" +
                "\n" +
                "INSERT INTO table (col1, col2) VALUES ('val1', 'val2'); ";

        String resultSql = SqlValidator.removeComments(sqlWithComments);
        Assertions.assertEquals(expectedSql, resultSql);
    }

    @Test
    @DisplayName("测试移除嵌套在字符串中的注释符号")
    void testRemoveCommentsInStrings() {
        String sqlWithStringComments = "SELECT * FROM users WHERE text = '-- not a comment' AND col = \"/* also not a comment */\"";

        // 注释符在字符串中不应被移除
        String result = SqlValidator.removeComments(sqlWithStringComments);
        Assertions.assertEquals(sqlWithStringComments, result);
    }

    @Test
    @DisplayName("测试读取SQL文件和分割SQL语句")
    void testReadSqlFile() throws IOException, JSQLParserException {
        List<String> sqlStatements = SqlValidator.readSqlFile(tempSqlFile.toString(), DbType.MYSQL);

        // 检查是否正确读取了三个SQL语句
        Assertions.assertEquals(3, sqlStatements.size());

        // 验证内容是否正确 (已移除注释)
        Assertions.assertTrue(sqlStatements.get(0).contains("SELECT * FROM users"));
        Assertions.assertTrue(sqlStatements.get(1).contains("INSERT INTO products"));
        Assertions.assertTrue(sqlStatements.get(2).contains("UPDATE orders"));
    }

    @ParameterizedTest(name = "测试{0}数据库特有SQL语法")
    @MethodSource("dbTypeAndSqlProvider")
    void testDatabaseSpecificSql(DbType dbType, String sql, boolean expected) {
        Assertions.assertEquals(expected, SqlValidator.isValidSql(sql, dbType));
    }

    static Stream<Arguments> dbTypeAndSqlProvider() {
        return Stream.of(
            // MySQL特有语法测试
            Arguments.of(DbType.MYSQL, "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY)", true),
            Arguments.of(DbType.MYSQL, "INSERT INTO users VALUES (1, 'John') ON DUPLICATE KEY UPDATE name='John'", true),

            // PostgreSQL特有语法测试
            Arguments.of(DbType.POSTGRESQL, "CREATE TABLE users (id SERIAL PRIMARY KEY)", true),
            Arguments.of(DbType.POSTGRESQL, "SELECT * FROM users WHERE id = 1", true),

            // GaussDB特有语法测试
            Arguments.of(DbType.GAUSSDB, "SELECT * FROM users WHERE id = 1", true),

            // SQLite特有语法测试
            Arguments.of(DbType.SQLITE, "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT)", true),
            Arguments.of(DbType.SQLITE, "SELECT date('now')", true)
        );
    }

    @ParameterizedTest(name = "测试不同SQL关键字 {0}")
    @ValueSource(strings = {
        "SELECT * FROM users",
        "INSERT INTO users VALUES (1, 'test')",
        "UPDATE users SET name = 'test' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "CREATE TABLE test (id INT)",
        "DROP TABLE test",
        "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
        "TRUNCATE TABLE users"
    })
    void testDifferentSqlKeywords(String sql) {
        Assertions.assertTrue(SqlValidator.isValidSql(sql, DbType.MYSQL));
    }

    @Test
    @DisplayName("测试分割SQL语句")
    void testSplitSqlStatements() throws JSQLParserException {
        String multiSql = "SELECT * FROM users; INSERT INTO logs VALUES (1, 'test'); UPDATE settings SET value = 'new'";
        List<String> statements = SqlValidator.splitSqlStatements(multiSql, DbType.MYSQL);

        Assertions.assertEquals(3, statements.size());
        Assertions.assertTrue(statements.get(0).contains("SELECT"));
        Assertions.assertTrue(statements.get(1).contains("INSERT"));
        Assertions.assertTrue(statements.get(2).contains("UPDATE"));
    }

    @Test
    @DisplayName("测试处理带分号的字符串中的SQL语句分割")
    void testSplitWithSemicolonsInStrings() throws JSQLParserException {
        // 字符串中的分号不应导致SQL语句分割
        String sql = "INSERT INTO messages (text) VALUES ('Hello; world'); SELECT * FROM users;";
        List<String> statements = SqlValidator.splitSqlStatements(sql, DbType.MYSQL);

        Assertions.assertEquals(2, statements.size());
        Assertions.assertTrue(statements.get(0).contains("'Hello; world'"));
        Assertions.assertTrue(statements.get(1).contains("SELECT"));
    }
}
