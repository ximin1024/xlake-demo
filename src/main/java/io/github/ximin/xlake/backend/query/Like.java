package io.github.ximin.xlake.backend.query;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class Like implements Expression {
    private final Expression column;
    private final Expression pattern;
    private final boolean caseSensitive;
    private transient Pattern compiledPattern;

    public Like(Expression column, Expression pattern, boolean caseSensitive) {
        this.column = column;
        this.pattern = pattern;
        this.caseSensitive = caseSensitive;
        this.compiledPattern = compilePattern(pattern);
    }

    public Like(Expression column, Expression pattern) {
        this(column, pattern, true);
    }

    private Pattern compilePattern(Expression patternExpr) {
        if (patternExpr instanceof Literal) {
            String patternStr = ((Literal) patternExpr).getValue().toString();

            // 转义正则特殊字符，除了 % 和 _
            String regex = patternStr
                    .replace("\\", "\\\\")
                    .replace(".", "\\.")
                    .replace("^", "\\^")
                    .replace("$", "\\$")
                    .replace("*", "\\*")
                    .replace("+", "\\+")
                    .replace("?", "\\?")
                    .replace("(", "\\(")
                    .replace(")", "\\)")
                    .replace("[", "\\[")
                    .replace("]", "\\]")
                    .replace("{", "\\{")
                    .replace("}", "\\}")
                    .replace("|", "\\|")
                    // 处理 LIKE 通配符
                    .replace("%", ".*")
                    .replace("_", ".");

            int flags = caseSensitive ? 0 : Pattern.CASE_INSENSITIVE;
            try {
                return Pattern.compile("^" + regex + "$", flags);
            } catch (PatternSyntaxException e) {
                // 如果模式无效，返回 null
                return null;
            }
        }
        return null;
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        Comparable columnValue = evaluateExpression(column, row);
        Comparable patternValue = evaluateExpression(pattern, row);

        if (columnValue == null || patternValue == null) {
            return false;
        }

        String value = columnValue.toString();
        String patternStr = patternValue.toString();

        // 编译模式（如果未编译）
        if (compiledPattern == null) {
            compiledPattern = compilePattern(pattern);
        }

        if (compiledPattern != null) {
            return compiledPattern.matcher(value).matches();
        }

        // 简单的字符串匹配（处理简单的 % 和 _）
        return simpleLikeMatch(value, patternStr, caseSensitive);
    }

    private boolean simpleLikeMatch(String value, String pattern, boolean caseSensitive) {
        if (!caseSensitive) {
            value = value.toLowerCase();
            pattern = pattern.toLowerCase();
        }

        int i = 0, j = 0;
        int patternLen = pattern.length();
        int valueLen = value.length();

        while (i < patternLen && j < valueLen) {
            char p = pattern.charAt(i);

            if (p == '%') {
                // 处理 % 通配符
                if (i == patternLen - 1) {
                    return true; // 以 % 结尾，匹配剩余所有字符
                }

                // 递归查找后续匹配
                String remainingPattern = pattern.substring(i + 1);
                for (int k = j; k <= valueLen; k++) {
                    if (simpleLikeMatch(value.substring(k), remainingPattern, true)) {
                        return true;
                    }
                }
                return false;
            } else if (p == '_') {
                // 下划线匹配单个字符
                i++;
                j++;
            } else if (p == '\\' && i + 1 < patternLen) {
                // 转义字符
                char nextChar = pattern.charAt(i + 1);
                if (nextChar == '%' || nextChar == '_' || nextChar == '\\') {
                    if (value.charAt(j) != nextChar) {
                        return false;
                    }
                    i += 2;
                    j++;
                } else {
                    if (value.charAt(j) != p) {
                        return false;
                    }
                    i++;
                    j++;
                }
            } else {
                // 普通字符
                if (value.charAt(j) != p) {
                    return false;
                }
                i++;
                j++;
            }
        }

        // 处理模式结束但还有 % 的情况
        while (i < patternLen && pattern.charAt(i) == '%') {
            i++;
        }

        return i == patternLen && j == valueLen;
    }

    private Comparable evaluateExpression(Expression expr, Map<String, Comparable> row) {
        if (expr instanceof ColumnRef) {
            String columnName = ((ColumnRef) expr).getColumnName();
            return row.get(columnName);
        } else if (expr instanceof Literal) {
            return ((Literal) expr).getValue();
        }
        return null;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.LIKE;
    }

    @Override
    public Set<String> getReferencedColumns() {
        Set<String> columns = new HashSet<>();
        columns.addAll(column.getReferencedColumns());
        columns.addAll(pattern.getReferencedColumns());
        return columns;
    }

    @Override
    public Expression simplify() {
        Expression simplifiedColumn = column.simplify();
        Expression simplifiedPattern = pattern.simplify();

        // 如果模式是字面量，检查是否有效
        if (simplifiedPattern instanceof Literal) {
            String patternStr = ((Literal) simplifiedPattern).getValue().toString();

            // 如果模式为空或只包含 %，总是匹配
            if (patternStr.isEmpty()) {
                return new Literal(true);
            }

            // 如果模式是 %，匹配任何非空字符串
            if ("%".equals(patternStr)) {
                return new IsNotNull(simplifiedColumn);
            }
        }

        return new Like(simplifiedColumn, simplifiedPattern, caseSensitive);
    }

    @Override
    public boolean alwaysTrue() {
        // 检查模式是否为 %
        if (pattern instanceof Literal) {
            String patternStr = ((Literal) pattern).getValue().toString();
            return "%".equals(patternStr);
        }
        return false;
    }

    @Override
    public boolean alwaysFalse() {
        // 如果列引用不存在，或者模式是无效的
        return false;
    }

    public Expression getColumn() {
        return column;
    }

    public Expression getPattern() {
        return pattern;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    @Override
    public Expression copy() {
        return new Like(column.copy(), pattern.copy(), caseSensitive);
    }

    @Override
    public String toString() {
        String caseStr = caseSensitive ? "" : " (case-insensitive)";
        return column.toString() + " LIKE " + pattern.toString() + caseStr;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Like that = (Like) obj;
        return caseSensitive == that.caseSensitive &&
                Objects.equals(column, that.column) &&
                Objects.equals(pattern, that.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, pattern, caseSensitive, getType());
    }
}
