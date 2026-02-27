package com.ibm.flexdata.splitter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SqlQueryLoader {

    public static class SqlQuery {
        private final String baseName;
        private final String sql;


        public SqlQuery(String baseName, String sql) {
            this.baseName = baseName;
            this.sql = sql;
        }

        public String baseName() {
            return baseName;
        }

        public String sql() {
            return sql;
        }
    }




    private final Path sqlDir;
    // Matches things like: INTERVAL '30 days'
    // and rewrites to:     INTERVAL '30' DAYS
    private static final Pattern INTERVAL_LITERAL_WITH_UNIT_IN_STRING =
        Pattern.compile("(?i)INTERVAL\\s+'\\s*(\\d+)\\s+([A-Za-z]+)\\s*'");




    public SqlQueryLoader(Path sqlDir) {
        this.sqlDir = sqlDir;
    }

    public List<SqlQuery> loadQueries() throws IOException {
        List<SqlQuery> queries = new ArrayList<>();

        try (DirectoryStream<Path> stream =
                 Files.newDirectoryStream(sqlDir, "*.sql")) {
            for (Path p : stream) {
                String fileName = p.getFileName().toString();
                int dot = fileName.lastIndexOf('.');
                String baseName = (dot > 0) ? fileName.substring(0, dot) : fileName;

                String rawSql = Files.readString(p, StandardCharsets.UTF_8);
                String sql    = preprocessSql(rawSql);

                queries.add(new SqlQuery(baseName, sql));
            }
        }

        return queries;
    }




    /**
     * Apply light, text-level rewrites to raw SQL files:
     *  1) Drop a single trailing ';' (plus trailing whitespace).
     *  2) Rename aliases "AS Rank" -> "AS Ran" (case-insensitive).
     */
    private static String preprocessSql(String raw) {
        if (raw == null) {
            return null;
        }

        String sql = raw.trim();

        // 1) Remove a trailing semicolon, if present
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }

        // 2) Rewrite alias "AS Rank" -> "AS Ran" (case-insensitive).
        sql = sql.replaceAll("(?i)\\bAS\\s+Rank\\b", "AS Ran");


        // 3) Fix INTERVAL '30 days' -> INTERVAL '30' DAYS
        sql = fixIntervalLiterals(sql);



        return sql;
    }




    private static String fixIntervalLiterals(String sql) {
        Matcher m = INTERVAL_LITERAL_WITH_UNIT_IN_STRING.matcher(sql);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String amount = m.group(1);                         
            String unit   = m.group(2).toUpperCase(Locale.ROOT); 
            String replacement = "INTERVAL '" + amount + "' " + unit;
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

}
