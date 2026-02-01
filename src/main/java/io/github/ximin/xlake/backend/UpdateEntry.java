package io.github.ximin.xlake.backend;

import io.github.ximin.xlake.backend.query.Expression;
import io.github.ximin.xlake.backend.query.ExpressionUtils;
import lombok.Getter;

import java.util.*;

public class UpdateEntry {
    @Getter
    private final Expression condition;
    private final Map<String, Comparable> updates;
    @Getter
    private final long timestamp;
    @Getter
    private final String entryId;

    public UpdateEntry(Expression condition, Map<String, Comparable> updates) {
        this.condition = condition;
        this.updates = new HashMap<>(updates);
        this.timestamp = System.currentTimeMillis();
        this.entryId = UUID.randomUUID().toString();
    }

    public boolean isPrimaryKeyUpdate(Set<String> primaryKeys) {
        return ExpressionUtils.isPrimaryKeyEquals(condition, primaryKeys);
    }

    public Map<String, Comparable> getPrimaryKeyValues(Set<String> primaryKeys) {
        return ExpressionUtils.extractPrimaryKeyValues(condition, primaryKeys);
    }

    public Map<String, Comparable> getUpdates() {
        return Collections.unmodifiableMap(updates);
    }

    @Override
    public String toString() {
        return String.format("UPDATE SET %s WHERE %s", updates, condition);
    }
}
