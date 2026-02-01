package io.github.ximin.xlake.backend;

import io.github.ximin.xlake.backend.query.Expression;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class QueryOptimization {
    private Expression directQuery;
    private Expression combinedQuery;
    private List<UpdateEntry> relevantUpdates;
    private List<Map<String, Comparable>> results;

    public boolean isRequiresMerge() {
        return combinedQuery != null;
    }
}
