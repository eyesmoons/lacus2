package com.lacus.flink.parser;

import com.lacus.core.flink.model.SqlCommandCall;
import com.lacus.flink.enums.SqlCommand;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;

public class SqlFileParser {


    public static List<String> parserSql(List<String> lineList) {
        if (CollectionUtils.isEmpty(lineList)) {
            throw new RuntimeException("lineList is null");
        }
        List<String> sqlList = new ArrayList<>();
        StringBuilder stmt = new StringBuilder();
        for (String line : lineList) {
            //开头是 -- 的表示注释
            if (line.trim().isEmpty() || line.startsWith("--") || trimStart(line).startsWith("--")) {
                continue;
            }
            stmt.append("\n").append(line);
            if (line.trim().endsWith(";")) {
                sqlList.add(stmt.substring(0, stmt.length() - 1));
                //初始化
                stmt.setLength(0);
            }
        }
        return sqlList;
    }


    public static List<SqlCommandCall> fileToSql(List<String> lineList) {
        if (CollectionUtils.isEmpty(lineList)) {
            throw new RuntimeException("lineList is null");
        }

        List<SqlCommandCall> sqlCommandCallList = new ArrayList<>();

        StringBuilder stmt = new StringBuilder();

        for (String line : lineList) {
            //开头是 -- 的表示注释
            if (line.trim().isEmpty() || line.startsWith("--") || trimStart(line).startsWith("--")) {
                continue;
            }
            stmt.append("\n").append(line);
            if (line.trim().endsWith(";")) {
                Optional<SqlCommandCall> optionalCall = parse(stmt.toString());
                if (optionalCall.isPresent()) {
                    sqlCommandCallList.add(optionalCall.get());
                } else {
                    throw new RuntimeException("不支持该语法使用" + stmt.toString() + "'");
                }
                stmt.setLength(0);
            }
        }
        return sqlCommandCallList;

    }

    private static Optional<SqlCommandCall> parse(String stmt) {
        stmt = stmt.trim();
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }
        for (SqlCommand cmd : SqlCommand.values()) {
            final Matcher matcher = cmd.getPattern().matcher(stmt);
            if (matcher.matches()) {
                final String[] groups = new String[matcher.groupCount()];
                for (int i = 0; i < groups.length; i++) {
                    groups[i] = matcher.group(i + 1);
                }
                return cmd.getOperandConverter().apply(groups)
                        .map((operands) -> new SqlCommandCall(cmd, operands));
            }
        }
        return Optional.empty();
    }


    private static String trimStart(String str) {
        if (StringUtils.isEmpty(str)) {
            return str;
        }
        final char[] value = str.toCharArray();

        int start = 0;
        int end = str.length() - 1;
        while ((start <= end) && (value[start] <= ' ')) {
            start++;
        }
        if (start == 0) {
            return str;
        }
        if (start >= end) {
            return "";
        }
        return str.substring(start, end);
    }
}
