/*-
 * #%L
 * xlake-demo
 * %%
 * Copyright (C) 2026 ximin1024
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package io.github.ximin.xlake.backend.query.serializer;

import io.github.ximin.xlake.backend.query.*;
import io.github.ximin.xlake.backend.query.assignment.*;
import io.github.ximin.xlake.meta.ExprType;
import io.github.ximin.xlake.meta.PbExpression;
import io.github.ximin.xlake.meta.PbLiteralValue;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public final class ProtoExpressionSerializer implements ExpressionSerializer {

    private static final ProtoExpressionSerializer INSTANCE = new ProtoExpressionSerializer();

    private ProtoExpressionSerializer() {}

    public static ProtoExpressionSerializer getInstance() {
        return INSTANCE;
    }

    @Override
    public byte[] serialize(io.github.ximin.xlake.backend.query.Expression expression) {
        return toBytes(expression);
    }

    @Override
    public io.github.ximin.xlake.backend.query.Expression deserialize(byte[] bytes) {
        return fromBytes(bytes);
    }

    public static PbExpression toProto(io.github.ximin.xlake.backend.query.Expression expr) {
        if (expr == null) return null;

        var builder = PbExpression.newBuilder()
                .setExprType(toProtoExprType(expr.getType()));

        switch (expr) {
            case Literal literal -> {
                builder.setLiteral(toProtoLiteral(literal));
            }
            case ColumnRef columnRef -> {
                builder.setColumnRef(columnRef.columnName());
            }
            case BinaryExpression binary -> {
                builder.addChildren(toProto(binary.getLeft()));
                builder.addChildren(toProto(binary.getRight()));
            }
            case LogicalExpression logical -> {
                for (io.github.ximin.xlake.backend.query.Expression child : logical.children()) {
                    builder.addChildren(toProto(child));
                }
            }
            case Between between -> {
                builder.addChildren(toProto(between.column()));
                builder.addChildren(toProto(between.lowerBound()));
                builder.addChildren(toProto(between.upperBound()));
            }
            case In inExpr -> {
                builder.addChildren(toProto(inExpr.column()));
                for (Comparable value : inExpr.values()) {
                    builder.addChildren(toProto(new Literal(value)));
                }
            }
            case NotIn notIn -> {
                builder.addChildren(toProto(notIn.column()));
                for (Comparable value : notIn.values()) {
                    builder.addChildren(toProto(new Literal(value)));
                }
            }
            case Like like -> {
                builder.addChildren(toProto(like.getColumn()));
                builder.addChildren(toProto(like.getPattern()));
                if (!like.isCaseSensitive()) {
                    builder.putMetadata("case_sensitive", "false");
                }
            }
            case IsNull isNull -> {
                builder.addChildren(toProto(isNull.getColumn()));
            }
            case IsNotNull isNotNull -> {
                builder.addChildren(toProto(isNotNull.getColumn()));
            }
            case Direct direct -> {
                builder.setColumnRef(direct.targetColumn());
                builder.addChildren(toProto(direct.valueExpression()));
            }
            case ArithmeticBase arithmetic -> {
                builder.putMetadata("target_column", arithmetic.targetColumn());
                builder.addChildren(toProto(arithmetic.getLeftOperand()));
                builder.addChildren(toProto(arithmetic.getRightOperand()));
            }
            default -> log.warn("Unhandled expression type: {}", expr.getType());
        }

        return builder.build();
    }

    public static io.github.ximin.xlake.backend.query.Expression fromProto(PbExpression proto) {
        if (proto == null) return null;

        ExprType type = proto.getExprType();

        return switch (type) {
            case LITERAL -> fromProtoLiteral(proto.getLiteral());
            case COLUMN_REF -> new ColumnRef(proto.getColumnRef());

            case EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL,
                 LESS_THAN, LESS_THAN_OR_EQUAL ->
                    fromBinaryComparison(type, proto.getChildrenList());

            case IN -> fromIn(proto.getChildrenList(), false);
            case NOT_IN -> fromIn(proto.getChildrenList(), true);
            case AND -> new And(fromProtoList(proto.getChildrenList()));
            case OR -> new Or(fromProtoList(proto.getChildrenList()));
            case NOT -> new Not(fromProtoSingle(proto.getChildrenList()));
            case BETWEEN -> fromBetween(proto.getChildrenList());
            case LIKE -> fromLike(proto.getChildrenList(), proto.getMetadataMap());
            case STARTS_WITH -> fromBinaryComparisonOp(StartWith::new, proto.getChildrenList());
            case ENDS_WITH -> fromBinaryComparisonOp(EndWith::new, proto.getChildrenList());
            case CONTAINS -> fromBinaryComparisonOp(Contains::new, proto.getChildrenList());
            case IS_NULL -> new IsNull(fromProtoSingle(proto.getChildrenList()));
            case IS_NOT_NULL -> new IsNotNull(fromProtoSingle(proto.getChildrenList()));
            case DIRECT -> new Direct(
                    proto.getColumnRef(),
                    fromProtoSingle(proto.getChildrenList())
            );

            case ARITHMETIC_ADD, ARITHMETIC_SUBTRACT,
                 ARITHMETIC_MULTIPLY, ARITHMETIC_DIVIDE ->
                    fromArithmetic(type, proto);

            default -> throw new IllegalArgumentException("Unknown expression type: " + type);
        };
    }

    public static byte[] toBytes(io.github.ximin.xlake.backend.query.Expression expr) {
        return toProto(expr).toByteArray();
    }

    public static io.github.ximin.xlake.backend.query.Expression fromBytes(byte[] data) {
        try {
            return fromProto(PbExpression.parseFrom(data));
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Expression", e);
        }
    }

    private static ExprType toProtoExprType(io.github.ximin.xlake.backend.query.Expression.ExpressionType type) {
        return switch (type) {
            case LITERAL -> ExprType.LITERAL;
            case COLUMN_REF -> ExprType.COLUMN_REF;
            case EQUAL -> ExprType.EQUAL;
            case NOT_EQUAL -> ExprType.NOT_EQUAL;
            case GREATER_THAN -> ExprType.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL -> ExprType.GREATER_THAN_OR_EQUAL;
            case LESS_THAN -> ExprType.LESS_THAN;
            case LESS_THAN_OR_EQUAL -> ExprType.LESS_THAN_OR_EQUAL;
            case IN -> ExprType.IN;
            case NOT_IN -> ExprType.NOT_IN;
            case AND -> ExprType.AND;
            case OR -> ExprType.OR;
            case NOT -> ExprType.NOT;
            case BETWEEN -> ExprType.BETWEEN;
            case LIKE -> ExprType.LIKE;
            case STARTS_WITH -> ExprType.STARTS_WITH;
            case ENDS_WITH -> ExprType.ENDS_WITH;
            case CONTAINS -> ExprType.CONTAINS;
            case IS_NULL -> ExprType.IS_NULL;
            case IS_NOT_NULL -> ExprType.IS_NOT_NULL;
            case DIRECT -> ExprType.DIRECT;
            case ARITHMETIC_ADD -> ExprType.ARITHMETIC_ADD;
            case ARITHMETIC_SUBTRACT -> ExprType.ARITHMETIC_SUBTRACT;
            case ARITHMETIC_MULTIPLY -> ExprType.ARITHMETIC_MULTIPLY;
            case ARITHMETIC_DIVIDE -> ExprType.ARITHMETIC_DIVIDE;
            default -> ExprType.EXPR_TYPE_UNSPECIFIED;
        };
    }

    private static PbLiteralValue toProtoLiteral(Literal literal) {
        Comparable value = literal.getValue();
        var builder = PbLiteralValue.newBuilder();
        if (value == null) {
            return builder.build();
        }

        if (value instanceof Boolean b) {
            return builder.setBoolVal(b).build();
        } else if (value instanceof Integer i) {
            return builder.setInt32Val(i).build();
        } else if (value instanceof Long l) {
            return builder.setInt64Val(l).build();
        } else if (value instanceof Float f) {
            return builder.setFloatVal(f).build();
        } else if (value instanceof Double d) {
            return builder.setDoubleVal(d).build();
        } else if (value instanceof String s) {
            return builder.setStringVal(s).build();
        } else {
            return builder.setStringVal(String.valueOf(value)).build();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T fromProtoLiteralValue(PbLiteralValue literal) {
        return switch (literal.getValueCase()) {
            case BOOL_VAL -> (T) Boolean.valueOf(literal.getBoolVal());
            case INT32_VAL -> (T) Integer.valueOf(literal.getInt32Val());
            case INT64_VAL -> (T) Long.valueOf(literal.getInt64Val());
            case FLOAT_VAL -> (T) Float.valueOf(literal.getFloatVal());
            case DOUBLE_VAL -> (T) Double.valueOf(literal.getDoubleVal());
            case STRING_VAL -> (T) literal.getStringVal();
            case BYTES_VAL -> (T) literal.getBytesVal().toByteArray();
            default -> null;
        };
    }

    private static Literal fromProtoLiteral(PbLiteralValue proto) {
        return new Literal(fromProtoLiteralValue(proto));
    }

    private static List<io.github.ximin.xlake.backend.query.Expression> fromProtoList(List<PbExpression> list) {
        List<io.github.ximin.xlake.backend.query.Expression> result = new ArrayList<>();
        for (PbExpression expr : list) {
            result.add(fromProto(expr));
        }
        return result;
    }

    private static io.github.ximin.xlake.backend.query.Expression fromProtoSingle(List<PbExpression> list) {
        if (list.isEmpty()) return null;
        return fromProto(list.getFirst());
    }

    private static BinaryComparison fromBinaryComparison(ExprType type, List<PbExpression> children) {
        if (children.size() < 2) {
            throw new IllegalArgumentException("Binary comparison needs 2 children");
        }
        var left = fromProto(children.get(0));
        var right = fromProto(children.get(1));

        return switch (type) {
            case EQUAL -> new Eq(left, right);
            case NOT_EQUAL -> new NotEq(left, right);
            case GREATER_THAN -> new Gt(left, right);
            case GREATER_THAN_OR_EQUAL -> new GtEq(left, right);
            case LESS_THAN -> new Lt(left, right);
            case LESS_THAN_OR_EQUAL -> new LtEq(left, right);
            default -> throw new IllegalArgumentException("Not a binary comparison: " + type);
        };
    }

    @FunctionalInterface
    private interface BiExprConstructor {
        BinaryComparison create(io.github.ximin.xlake.backend.query.Expression left, io.github.ximin.xlake.backend.query.Expression right);
    }

    private static BinaryComparison fromBinaryComparisonOp(BiExprConstructor ctor, List<PbExpression> children) {
        if (children.size() < 2) {
            throw new IllegalArgumentException("Binary operation needs 2 children");
        }
        return ctor.create(fromProto(children.get(0)), fromProto(children.get(1)));
    }

    private static io.github.ximin.xlake.backend.query.Expression fromIn(List<PbExpression> children, boolean negated) {
        if (children.isEmpty()) return null;
        var column = fromProto(children.get(0));
        Set<Comparable> values = new HashSet<>();
        for (int i = 1; i < children.size(); i++) {
            var lit = fromProto(children.get(i));
            if (lit instanceof Literal l && l.getValue() != null) {
                values.add(l.getValue());
            }
        }
        return negated ? new NotIn(column, values) : new In(column, values);
    }

    private static Between fromBetween(List<PbExpression> children) {
        if (children.size() < 3) {
            throw new IllegalArgumentException("Between needs 3 children");
        }
        return new Between(
                fromProto(children.get(0)),
                fromProto(children.get(1)),
                fromProto(children.get(2))
        );
    }

    private static Like fromLike(List<PbExpression> children, Map<String, String> metadata) {
        if (children.size() < 2) {
            throw new IllegalArgumentException("Like needs at least 2 children");
        }
        var column = fromProto(children.get(0));
        var pattern = fromProto(children.get(1));
        boolean caseSensitive = !"false".equals(metadata.getOrDefault("case_sensitive", "true"));
        return new Like(column, pattern, caseSensitive);
    }

    private static Assignment fromArithmetic(ExprType type, PbExpression proto) {
        String targetColumn = proto.getMetadataOrDefault("target_column", "");
        List<PbExpression> children = proto.getChildrenList();
        if (children.size() < 2) {
            throw new IllegalArgumentException("Arithmetic needs 2 operand children");
        }
        var left = fromProto(children.get(0));
        var right = fromProto(children.get(1));

        return switch (type) {
            case ARITHMETIC_ADD -> new Add(targetColumn, left, right);
            case ARITHMETIC_SUBTRACT -> new Subtract(targetColumn, left, right);
            case ARITHMETIC_MULTIPLY -> new Multiply(targetColumn, left, right);
            case ARITHMETIC_DIVIDE -> new Divide(targetColumn, left, right);
            default -> throw new IllegalArgumentException("Unknown arithmetic type: " + type);
        };
    }
}
