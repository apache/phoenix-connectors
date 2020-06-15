/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.query;


import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.ql.index.IndexSearchCondition;
import org.apache.phoenix.hive.util.ColumnMappingUtils;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;
import org.apache.phoenix.util.StringUtil;

import static org.apache.phoenix.hive.util.ColumnMappingUtils.getColumnMappingMap;

/**
 * Query builder. Produces a query depending on the colummn list and conditions
 */

public class PhoenixQueryBuilder {

    private static final Log LOG = LogFactory.getLog(PhoenixQueryBuilder.class);

    private static final String QUERY_TEMPLATE = "select $HINT$ $COLUMN_LIST$ from $TABLE_NAME$";

    private static final PhoenixQueryBuilder QUERY_BUILDER = new PhoenixQueryBuilder();

    private PhoenixQueryBuilder() {
        if (LOG.isInfoEnabled()) {
            LOG.info("PhoenixQueryBuilder created");
        }
    }

    public static PhoenixQueryBuilder getInstance() {
        return QUERY_BUILDER;
    }

    private void addConditionColumnToReadColumn(List<String> readColumnList, List<String>
            conditionColumnList) {
        if (readColumnList.isEmpty()) {
            return;
        }

        for (String conditionColumn : conditionColumnList) {
            if (!readColumnList.contains(conditionColumn)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Condition column " + conditionColumn + " does not exist in " +
                            "read-columns.");
                }

                readColumnList.add(conditionColumn);
            }
        }
    }

    private static String findReplacement(JobConf jobConf, String column) {
        Map<String, String> columnMappingMap = getColumnMappingMap(jobConf.get
                (PhoenixStorageHandlerConstants.PHOENIX_COLUMN_MAPPING));
        if (columnMappingMap != null && columnMappingMap.containsKey(column)) {
            return columnMappingMap.get(column);
        } else {
            return column;
        }
    }
    private static List<String> replaceColumns(JobConf jobConf, List<String> columnList) {
        Map<String, String> columnMappingMap = getColumnMappingMap(jobConf.get
                (PhoenixStorageHandlerConstants.PHOENIX_COLUMN_MAPPING));
        if(columnMappingMap != null) {
          List<String> newList = new ArrayList<>();
            for(String column:columnList) {
                if(columnMappingMap.containsKey(column)) {
                    newList.add(columnMappingMap.get(column));
                } else {
                    newList.add(column);
                }
            }
            return newList;
        }
        return null;
    }

    private String makeQueryString(JobConf jobConf, String tableName, List<String>
            readColumnList, List<IndexSearchCondition> searchConditions, String queryTemplate,
                                   String hints) throws IOException {
        StringBuilder query = new StringBuilder();
        List<String> conditionColumnList = buildWhereClause(jobConf, query, searchConditions);

        if (conditionColumnList.size() > 0) {
            readColumnList  = replaceColumns(jobConf, readColumnList);
            addConditionColumnToReadColumn(readColumnList, conditionColumnList);
            query.insert(0, queryTemplate.replace("$HINT$", hints).replace("$COLUMN_LIST$",
                    getSelectColumns(jobConf, tableName, readColumnList)).replace("$TABLE_NAME$",
                    tableName));
        } else {
            readColumnList  = replaceColumns(jobConf, readColumnList);
            query.append(queryTemplate.replace("$HINT$", hints).replace("$COLUMN_LIST$",
                    getSelectColumns(jobConf, tableName, readColumnList)).replace("$TABLE_NAME$",
                    tableName));
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Input query : " + query.toString());
        }

        return query.toString();
    }

    private String getSelectColumns(JobConf jobConf, String tableName, List<String>
            readColumnList) throws IOException {
        String selectColumns = String.join(PhoenixStorageHandlerConstants.COMMA,
            ColumnMappingUtils.quoteColumns(readColumnList));
        if (PhoenixStorageHandlerConstants.EMPTY_STRING.equals(selectColumns)) {
            selectColumns = "*";
        } else {
            if (PhoenixStorageHandlerUtil.isTransactionalTable(jobConf)) {
                List<String> pkColumnList = PhoenixUtil.getPrimaryKeyColumnList(jobConf, tableName);
                StringBuilder pkColumns = new StringBuilder();

                for (String pkColumn : pkColumnList) {
                    if (!readColumnList.contains(pkColumn)) {
                        pkColumns.append("\"").append(pkColumn).append("\"" + PhoenixStorageHandlerConstants.COMMA);
                    }
                }

                selectColumns = pkColumns.toString() + selectColumns;
            }
        }

        return selectColumns;
    }

    public String buildQuery(JobConf jobConf, String tableName, List<String> readColumnList,
                             List<IndexSearchCondition> searchConditions) throws IOException {
        String hints = getHint(jobConf, tableName);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Building query with columns : " + readColumnList + "  table name : " +
                    tableName + " search conditions : " + searchConditions + "  hints : " + hints);
        }

        return makeQueryString(jobConf, tableName,  new ArrayList<>(readColumnList),
                searchConditions, QUERY_TEMPLATE, hints);
    }

    private String getHint(JobConf jobConf, String tableName) {
        StringBuilder hints = new StringBuilder("/*+ ");
        if (!jobConf.getBoolean(PhoenixStorageHandlerConstants.HBASE_SCAN_CACHEBLOCKS, Boolean
                .FALSE)) {
            hints.append("NO_CACHE ");
        }

        String queryHint = jobConf.get(tableName + PhoenixStorageHandlerConstants
                .PHOENIX_TABLE_QUERY_HINT);
        if (queryHint != null) {
            hints.append(queryHint);
        }
        hints.append(" */");

        return hints.toString();
    }

    protected List<String> buildWhereClause(JobConf jobConf, StringBuilder sql,
                                            List<IndexSearchCondition> conditions)
            throws IOException {
        if (conditions == null || conditions.size() == 0) {
            return Collections.emptyList();
        }

        List<String> columns = new ArrayList<>();
        sql.append(" where ");

        Iterator<IndexSearchCondition> iter = conditions.iterator();
        appendExpression(jobConf, sql, iter.next(), columns);
        while (iter.hasNext()) {
            sql.append(" and ");
            appendExpression(jobConf, sql, iter.next(), columns);
        }

        return columns;
    }

    private void appendExpression(JobConf jobConf, StringBuilder sql, IndexSearchCondition condition,
                                  List<String> columns) {
        Expression expr = findExpression(condition);
        if (expr != null) {
            sql.append(expr.buildExpressionStringFrom(jobConf, condition));
            String column = condition.getColumnDesc().getColumn();
            String rColumn = findReplacement(jobConf, column);
            if(rColumn != null) {
                column = rColumn;
            }

            columns.add(column);
        }
    }

    private Expression findExpression(final IndexSearchCondition condition) {
        return Iterables.tryFind(Arrays.asList(Expression.values()), new Predicate<Expression>() {
            @Override
            public boolean apply(@Nullable Expression expr) {
                return expr.isFor(condition);
            }
        }).orNull();
    }

    private static final Joiner JOINER_COMMA = Joiner.on(", ");
    private static final Joiner JOINER_AND = Joiner.on(" and ");
    private static final Joiner JOINER_SPACE = Joiner.on(" ");

    private enum Expression {
        EQUAL("UDFOPEqual", "="),
        GREATER_THAN_OR_EQUAL_TO("UDFOPEqualOrGreaterThan", ">="),
        GREATER_THAN("UDFOPGreaterThan", ">"),
        LESS_THAN_OR_EQUAL_TO("UDFOPEqualOrLessThan", "<="),
        LESS_THAN("UDFOPLessThan", "<"),
        NOT_EQUAL("UDFOPNotEqual", "!="),
        BETWEEN("GenericUDFBetween", "between", JOINER_AND, true) {
            public boolean checkCondition(IndexSearchCondition condition) {
                return condition.getConstantDescs() != null;
            }
        },
        IN("GenericUDFIn", "in", JOINER_COMMA, true) {
            public boolean checkCondition(IndexSearchCondition condition) {
                return condition.getConstantDescs() != null;
            }

            public String createConstants(final String typeName, ExprNodeConstantDesc[] desc) {
                return "(" + super.createConstants(typeName, desc) + ")";
            }
        },
        IS_NULL("GenericUDFOPNull", "is null") {
            public boolean checkCondition(IndexSearchCondition condition) {
                return true;
            }
        },
        IS_NOT_NULL("GenericUDFOPNotNull", "is not null") {
            public boolean checkCondition(IndexSearchCondition condition) {
                return true;
            }
        };

        private final String hiveCompOp;
        private final String sqlCompOp;
        private final Joiner joiner;
        private final boolean supportNotOperator;

        Expression(String hiveCompOp, String sqlCompOp) {
            this(hiveCompOp, sqlCompOp, null);
        }

        Expression(String hiveCompOp, String sqlCompOp, Joiner joiner) {
            this(hiveCompOp, sqlCompOp, joiner, false);
        }

        Expression(String hiveCompOp, String sqlCompOp, Joiner joiner, boolean supportNotOp) {
            this.hiveCompOp = hiveCompOp;
            this.sqlCompOp = sqlCompOp;
            this.joiner = joiner;
            this.supportNotOperator = supportNotOp;
        }

        public boolean checkCondition(IndexSearchCondition condition) {
            return condition.getConstantDesc().getValue() != null;
        }

        public boolean isFor(IndexSearchCondition condition) {
            return condition.getComparisonOp().endsWith(hiveCompOp) && checkCondition(condition);
        }

        public String buildExpressionStringFrom(JobConf jobConf, IndexSearchCondition condition) {
            final String type = condition.getColumnDesc().getTypeString();
            String column = condition.getColumnDesc().getColumn();
            String rColumn = findReplacement(jobConf, column);
            if(rColumn != null) {
                column = rColumn;
            }
            return JOINER_SPACE.join(
                    "\"" + column + "\"",
                    getSqlCompOpString(condition),
                    joiner != null ? createConstants(type, condition.getConstantDescs()) :
                            createConstant(type, condition.getConstantDesc()));
        }

        public String getSqlCompOpString(IndexSearchCondition condition) {
            return supportNotOperator ?
                    (condition.isNot() ? "not " : "") + sqlCompOp : sqlCompOp;
        }

        public String createConstant(String typeName, ExprNodeConstantDesc constantDesc) {
            if (constantDesc == null) {
                return StringUtil.EMPTY_STRING;
            }

            return createConstantString(typeName, String.valueOf(constantDesc.getValue()));
        }

        public String createConstants(final String typeName, ExprNodeConstantDesc[] constantDesc) {
            if (constantDesc == null) {
                return StringUtil.EMPTY_STRING;
            }

            return joiner.join(Iterables.transform(Arrays.asList(constantDesc),
                    new Function<ExprNodeConstantDesc, String>() {
                        @Nullable
                        @Override
                        public String apply(@Nullable ExprNodeConstantDesc desc) {
                            return createConstantString(typeName, String.valueOf(desc.getValue()));
                        }
                    }
            ));
        }

        private static class ConstantStringWrapper {
            private List<String> types;
            private String prefix;
            private String postfix;

            ConstantStringWrapper(String type, String prefix, String postfix) {
                this(new ArrayList<>(Arrays.asList(type)), prefix, postfix);
            }

            ConstantStringWrapper(List<String> types, String prefix, String postfix) {
                this.types = types;
                this.prefix = prefix;
                this.postfix = postfix;
            }

            public String apply(final String typeName, String value) {
                Iterables
                return Iterables.any(types, new Predicate<String>() {

                    @Override
                    public boolean apply(@Nullable String type) {
                        return typeName.startsWith(type);
                    }
                }) ? prefix + value + postfix : value;
            }
        }

        private static final String SINGLE_QUOTATION = "'";
        private static List<ConstantStringWrapper> WRAPPERS = Lists.newArrayList(
                new ConstantStringWrapper(Lists.newArrayList(
                        serdeConstants.STRING_TYPE_NAME, serdeConstants.CHAR_TYPE_NAME,
                        serdeConstants.VARCHAR_TYPE_NAME, serdeConstants.DATE_TYPE_NAME,
                        serdeConstants.TIMESTAMP_TYPE_NAME
                ), SINGLE_QUOTATION, SINGLE_QUOTATION),
                new ConstantStringWrapper(serdeConstants.DATE_TYPE_NAME, "to_date(", ")"),
                new ConstantStringWrapper(serdeConstants.TIMESTAMP_TYPE_NAME, "to_timestamp(", ")")
        );

        private String createConstantString(String typeName, String value) {
            for (ConstantStringWrapper wrapper : WRAPPERS) {
                value = wrapper.apply(typeName, value);
            }

            return value;
        }
    }
}
