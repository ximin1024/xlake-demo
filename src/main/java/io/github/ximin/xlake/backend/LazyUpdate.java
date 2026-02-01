package io.github.ximin.xlake.backend;

import io.github.ximin.xlake.backend.query.And;
import io.github.ximin.xlake.backend.query.Expression;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class LazyUpdate {
    private final Set<String> primaryKeys;
    private final List<UpdateEntry> pendingUpdates = new ArrayList<>();
    private final Map<String, Map<String, Comparable>> lsmTree = new HashMap<>();

    public LazyUpdate(Set<String> primaryKeys) {
        this.primaryKeys = new HashSet<>(primaryKeys);
    }

    public void processUpdate(UpdateEntry update) {
        if (update.isPrimaryKeyUpdate(primaryKeys)) {
            // 主键等值更新：写入 LSM Tree
            Map<String, Comparable> pkValues = update.getPrimaryKeyValues(primaryKeys);
            String pkString = serializePrimaryKey(pkValues);

            // 合并更新到现有数据
            Map<String, Comparable> existing = lsmTree.getOrDefault(pkString, new HashMap<>());
            existing.putAll(update.getUpdates());
            existing.put("_version", System.currentTimeMillis());
            lsmTree.put(pkString, existing);

            log.info("LSM Update: {}  -> {}", pkString, existing);
        } else {
            // 非主键范围更新：记录谓词条件
            pendingUpdates.add(update);
            log.info("Pending Update: {}", update);
        }
    }

    // 查询数据（合并存储的谓词条件）
    public List<Map<String, Comparable>> query(Expression where) {
        List<Map<String, Comparable>> results = new ArrayList<>();

        // 首先检查 LSM Tree 中的数据
        for (Map.Entry<String, Map<String, Comparable>> entry : lsmTree.entrySet()) {
            Map<String, Comparable> row = entry.getValue();

            if (where.evaluate(row)) {
                results.add(new HashMap<>(row));
            }
        }

        /*
         todo 对于未在 LSM Tree 中的数据，应用 pending updates,这里需要根据具体的数据源实现数据扫描,
          实际实现中会从底层存储（如 Parquet 文件）读取数据
         */

        return results;
    }

    // 查询优化的版本：合并谓词条件
    public QueryOptimization queryWithOptimization(Expression queryCondition) {
        QueryOptimization result = new QueryOptimization();

        // 分析查询条件
        Set<String> referencedColumns = queryCondition.getReferencedColumns();

        List<UpdateEntry> relevantUpdates = pendingUpdates.stream()
                .filter(update -> update.getCondition() != null)
                .filter(update -> hasOverlap(update.getCondition(), queryCondition))
                .collect(Collectors.toList());

        // 构建合并的查询计划
        if (relevantUpdates.isEmpty()) {
            result.setDirectQuery(queryCondition);
        } else {
            Expression combinedCondition = buildCombinedCondition(queryCondition, relevantUpdates);
            result.setCombinedQuery(combinedCondition);
            result.setRelevantUpdates(relevantUpdates);
        }

        return result;
    }

    private boolean hasOverlap(Expression updateCondition, Expression queryCondition) {
        // 简化实现：检查是否有共同的列引用
        Set<String> updateColumns = updateCondition.getReferencedColumns();
        Set<String> queryColumns = queryCondition.getReferencedColumns();

        return !Collections.disjoint(updateColumns, queryColumns);
    }

    private Expression buildCombinedCondition(Expression queryCondition, List<UpdateEntry> relevantUpdates) {
        // todo
        // 构建复杂的合并逻辑
        // 例如：
        // 查询: age > 8
        // 更新1: class = 4 where age > 10
        // 更新2: class = 5 where age > 20

        // 合并后的逻辑应该是：
        // 对于 age > 20 的数据，class = 5
        // 对于 10 < age <= 20 的数据，class = 4
        // 对于 8 < age <= 10 的数据，使用原始 class 值

        // 简化实现：返回 AND 条件
        List<Expression> conditions = new ArrayList<>();
        conditions.add(queryCondition);

        for (UpdateEntry update : relevantUpdates) {
            conditions.add(update.getCondition());
        }

        return new And(conditions.toArray(new Expression[0]));
    }

    private String serializePrimaryKey(Map<String, Comparable> pkValues) {
        return pkValues.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("|"));
    }

    public List<UpdateEntry> getPendingUpdates() {
        return Collections.unmodifiableList(pendingUpdates);
    }

    public Map<String, Map<String, Comparable>> getLsmTree() {
        return Collections.unmodifiableMap(lsmTree);
    }
}
