package com.ibm.flexdata.splitter;

import java.util.regex.Pattern;


public final class SqlPostProcessor {

    private static final Pattern ANY_VALUE_FUNC =
        Pattern.compile("(?i)ANY_VALUE\\s*\\(");

    // TIMESTAMP(0) -> TIMESTAMP
    private static final Pattern TIMESTAMP_PRECISION =
        Pattern.compile("(?i)TIMESTAMP\\s*\\(\\s*0\\s*\\)");

    // strip CHARACTER SET <something> after a type
    private static final Pattern CHARSET_CLAUSE =
        Pattern.compile("(?i)\\s+CHARACTER\\s+SET\\s+(\"[^\"]+\"|'[^']+'|\\S+)");


    // matches "$something"
    private static final Pattern LEADING_DOLLAR_IDENT =
    Pattern.compile("\"\\$([^\"]+)\"");  


    // SQL: FETCH NEXT|FIRST n ROWS ONLY  ->  LIMIT n
    private static final Pattern FETCH_LIMIT =
    Pattern.compile("(?i)FETCH\\s+(?:NEXT|FIRST)\\s+(\\d+)\\s+ROWS\\s+ONLY");


    private SqlPostProcessor() {}


    public static String normalize(String sql) {
        return normalize(sql, null);
    }




    /** Normalize SQL string for our execution engines. */
    public static String normalize(String sql, Engine engine) {
        return normalize(sql, engine, null);
    }

    /** Normalize SQL string for our execution engines. */
    public static String normalize(String sql, Engine engine, String schemaName) {
        if (sql == null) {
            return null;
        }
        String s = sql;

        // 1) Strip schema qualifier for selected schema (e.g., "STACK". or "IMDB".).
        s = stripSchemaQualifier(s, schemaName);

        // 2) Engine-specific fixes
        if (engine == Engine.DATAFUSION) {
            s = normalizeForDatafusion(s);
        } else if (engine == Engine.DUCKDB) {
            s = normalizeForDuckdb(s);
        }

        return s;
    }





    private static String normalizeForDuckdb(String sql) {
        String s = sql;

        // 1) Flatten extra parentheses around comma-separated lists: ,(( ... )) -> ,( ... )
        s = flattenDoubleParensAfterComma(s);

        // 2) TIMESTAMP(0) -> TIMESTAMP
        s = TIMESTAMP_PRECISION.matcher(s).replaceAll("TIMESTAMP");

        // 3) Drop CHARACTER SET clause in casts
        s = CHARSET_CLAUSE.matcher(s).replaceAll("");
        
        // 4) Rename variables starting with $
        s = LEADING_DOLLAR_IDENT.matcher(s).replaceAll("\"FD_D_$1\"");

        // FETCH -> LIMIT
        s = FETCH_LIMIT.matcher(s).replaceAll("LIMIT $1");
        
        return s;
    }




    private static String normalizeForDatafusion(String sql) {
        String s = sql;

        //  ANY_VALUE(x) -> MIN(x)
        s = ANY_VALUE_FUNC.matcher(s).replaceAll("MIN(");

        // TIMESTAMP(0) -> TIMESTAMP
        s = TIMESTAMP_PRECISION.matcher(s).replaceAll("TIMESTAMP");

        //  Drop CHARACTER SET clause in casts
        s = CHARSET_CLAUSE.matcher(s).replaceAll("");
        
        // Rename variables starting with $.  "$f9" -> "FD_D_f9"
        s = LEADING_DOLLAR_IDENT.matcher(s).replaceAll("\"FD_D_$1\"");

        // FETCH -> LIMIT
        s = FETCH_LIMIT.matcher(s).replaceAll("LIMIT $1");

        return s;
    }

    private static String stripSchemaQualifier(String sql, String schemaName) {
        String schema = (schemaName == null || schemaName.isBlank())
            ? Dataset.STACKOVERFLOW.getSchemaName()
            : schemaName.trim();
        String s = sql;
        Pattern quoted = Pattern.compile("(?i)\"" + Pattern.quote(schema) + "\"\\.");
        Pattern unquoted = Pattern.compile("(?i)\\b" + Pattern.quote(schema) + "\\.");
        s = quoted.matcher(s).replaceAll("");
        s = unquoted.matcher(s).replaceAll("");
        return s;
    }




    /**
     * Flatten patterns of the form ",(( ... ))" into ",( ... )" by removing
     * one extra pair of parentheses around a comma-separated item list.
     */
    private static String flattenDoubleParensAfterComma(String sql) {
        String s = sql;

        while (true) {
            int n = s.length();
            int commaIdx = -1;
            int outerOpen = -1;

            // Find ", ((" pattern: comma, optional whitespace, then "(("
            for (int i = 0; i < n; i++) {
                if (s.charAt(i) == ',') {
                    int j = i + 1;
                    // skip whitespace
                    while (j < n && Character.isWhitespace(s.charAt(j))) {
                        j++;
                    }
                    if (j + 1 < n && s.charAt(j) == '(' && s.charAt(j + 1) == '(') {
                        commaIdx = i;
                        outerOpen = j;   // this '(' is the outer one we want to remove
                        break;
                    }
                }
            }

            if (commaIdx < 0 || outerOpen < 0) {
                // no ",((" pattern found any more
                break;
            }

            int closeIdx = findMatchingParen(s, outerOpen);
            if (closeIdx < 0) {
                break;
            }


            String before = s.substring(0, outerOpen);
            String inside = s.substring(outerOpen + 1, closeIdx);   // keep everything inside, including the inner "("
            String after  = s.substring(closeIdx + 1);

            s = before + inside + after;
        }

        return s;
    }


    
    /**
     * Given a string and the index of an opening '(', find the index of the
     * matching closing ')', using a simple depth counter.
     *
     * Returns -1 if no matching paren is found (unbalanced).
     */
    private static int findMatchingParen(String s, int openIdx) {
        int depth = 0;
        int n = s.length();

        for (int i = openIdx; i < n; i++) {
            char c = s.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1; // unbalanced
    }

}
