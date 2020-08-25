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
package org.apache.phoenix.hive.ql.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.phoenix.hive.util.TypeInfoUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * Clone of org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer with modifying
 * analyzePredicate method.
 *
 *
 */
public class IndexPredicateAnalyzer {

    private static final Log LOG = LogFactory.getLog(IndexPredicateAnalyzer.class);

    private final Set<String> udfNames;
    private final Map<String, Set<String>> columnToUDFs;
    private FieldValidator fieldValidator;

    private boolean acceptsFields;

    public IndexPredicateAnalyzer() {
        udfNames = new HashSet<String>();
        columnToUDFs = new HashMap<String, Set<String>>();
    }

    public void setFieldValidator(FieldValidator fieldValidator) {
        this.fieldValidator = fieldValidator;
    }

    /**
     * Registers a comparison operator as one which can be satisfied by an index
     * search. Unless this is called, analyzePredicate will never find any
     * indexable conditions.
     *
     * @param udfName name of comparison operator as returned by either
     *                {@link GenericUDFBridge#getUdfName} (for simple UDF's) or
     *                udf.getClass().getName() (for generic UDF's).
     */
    public void addComparisonOp(String udfName) {
        udfNames.add(udfName);
    }

    /**
     * Clears the set of column names allowed in comparisons. (Initially, all
     * column names are allowed.)
     */
    public void clearAllowedColumnNames() {
        columnToUDFs.clear();
    }

    /**
     * Adds a column name to the set of column names allowed.
     *
     * @param columnName name of column to be allowed
     */
    public void allowColumnName(String columnName) {
        columnToUDFs.put(columnName, udfNames);
    }

    /**
     * add allowed functions per column
     *
     * @param columnName
     * @param udfs
     */
    public void addComparisonOp(String columnName, String... udfs) {
        Set<String> allowed = columnToUDFs.get(columnName);
        if (allowed == null || allowed == udfNames) {
            // override
            columnToUDFs.put(columnName, new HashSet<String>(Arrays.asList(udfs)));
        } else {
            allowed.addAll(Arrays.asList(udfs));
        }
    }

    /**
     * Analyzes a predicate.
     *
     * @param predicate        predicate to be analyzed
     * @param searchConditions receives conditions produced by analysis
     * @return residual predicate which could not be translated to
     * searchConditions
     */
    public ExprNodeDesc analyzePredicate(ExprNodeDesc predicate, final List<IndexSearchCondition>
            searchConditions) {

        Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
        NodeProcessor nodeProcessor = new NodeProcessor() {
            @Override
            public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object...
                    nodeOutputs) throws SemanticException {

                // We can only push down stuff which appears as part of
                // a pure conjunction: reject OR, CASE, etc.
                for (Node ancestor : stack) {
                    if (nd == ancestor) {
                        break;
                    }
                    if (!FunctionRegistry.isOpAnd((ExprNodeDesc) ancestor)) {
                        return nd;
                    }
                }

                return analyzeExpr((ExprNodeGenericFuncDesc) nd, searchConditions, nodeOutputs);
            }
        };

        Dispatcher disp = new DefaultRuleDispatcher(nodeProcessor, opRules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);
        ArrayList<Node> topNodes = new ArrayList<Node>();
        topNodes.add(predicate);
        HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();

        try {
            ogw.startWalking(topNodes, nodeOutput);
        } catch (SemanticException ex) {
            throw new RuntimeException(ex);
        }

        ExprNodeDesc residualPredicate = (ExprNodeDesc) nodeOutput.get(predicate);
        return residualPredicate;
    }

    // Check if ExprNodeColumnDesc is wrapped in expr.
    // If so, peel off. Otherwise return itself.
    private ExprNodeDesc getColumnExpr(ExprNodeDesc expr) {
        if (expr instanceof ExprNodeColumnDesc) {
            return expr;
        }
        ExprNodeGenericFuncDesc funcDesc = null;
        if (expr instanceof ExprNodeGenericFuncDesc) {
            funcDesc = (ExprNodeGenericFuncDesc) expr;
        }
        if (null == funcDesc) {
            return expr;
        }
        GenericUDF udf = funcDesc.getGenericUDF();
        // check if its a simple cast expression.
        if ((udf instanceof GenericUDFBridge || udf instanceof GenericUDFToBinary || udf
                instanceof GenericUDFToChar
                || udf instanceof GenericUDFToVarchar || udf instanceof GenericUDFToDecimal
                || udf instanceof GenericUDFToDate || udf instanceof GenericUDFToUnixTimeStamp
                || udf instanceof GenericUDFToUtcTimestamp) && funcDesc.getChildren().size() == 1
                && funcDesc.getChildren().get(0) instanceof ExprNodeColumnDesc) {
            return expr.getChildren().get(0);
        }
        return expr;
    }

    private void processingBetweenOperator(ExprNodeGenericFuncDesc expr,
                                           List<IndexSearchCondition> searchConditions, Object...
                                                   nodeOutputs) {
        String[] fields = null;

        final boolean isNot = (Boolean) ((ExprNodeConstantDesc) nodeOutputs[0]).getValue();
        ExprNodeDesc columnNodeDesc = (ExprNodeDesc) nodeOutputs[1];

        if (columnNodeDesc instanceof ExprNodeFieldDesc) {
            // rowKey field
            ExprNodeFieldDesc fieldDesc = (ExprNodeFieldDesc) columnNodeDesc;
            fields = ExprNodeDescUtils.extractFields(fieldDesc);

            ExprNodeDesc[] extracted = ExprNodeDescUtils.extractComparePair((ExprNodeDesc)
                    nodeOutputs[1], (ExprNodeDesc) nodeOutputs[2]);
            columnNodeDesc = extracted[0];
        }
        addSearchConditionIfPossible(expr, searchConditions, fields, isNot, columnNodeDesc,
                Arrays.copyOfRange(nodeOutputs, 2, nodeOutputs.length));
    }

    private void addSearchConditionIfPossible(ExprNodeGenericFuncDesc expr,
                                              List<IndexSearchCondition> searchConditions,
                                              String[] fields,
                                              boolean isNot,
                                              ExprNodeDesc columnNodeDesc,
                                              Object[] nodeOutputs) {
        ExprNodeColumnDesc columnDesc;
        columnNodeDesc = getColumnExpr(columnNodeDesc);
        if (!(columnNodeDesc instanceof ExprNodeColumnDesc)) {
            return;
        }
        columnDesc = (ExprNodeColumnDesc) columnNodeDesc;

        String udfName = expr.getGenericUDF().getUdfName();
        ExprNodeConstantDesc[] constantDescs = null;
        if (nodeOutputs != null) {
            constantDescs = extractConstants(columnDesc, nodeOutputs);
            if (constantDescs == null) {
                return;
            }
        }

        searchConditions.add(new IndexSearchCondition(columnDesc, udfName, constantDescs,
                expr, fields, isNot));
    }

    private boolean isAcceptableConstants(ExprNodeDesc columnDesc, ExprNodeDesc constant) {
        // from(constant) -> to(columnDesc)
        return TypeInfoUtils.implicitConvertible(constant.getTypeInfo(), columnDesc.getTypeInfo());
    }

    private ExprNodeConstantDesc[] extractConstants(ExprNodeColumnDesc columnDesc, Object... nodeOutputs) {
        ExprNodeConstantDesc[] constantDescs = new ExprNodeConstantDesc[nodeOutputs.length];
        for (int i = 0; i < nodeOutputs.length; i++) {
            ExprNodeDesc[] extracted =
                    ExprNodeDescUtils.extractComparePair(columnDesc, (ExprNodeDesc) nodeOutputs[i]);
            if (extracted == null || !isAcceptableConstants(columnDesc, extracted[1])) {
                return null;
            }
            constantDescs[i] = (ExprNodeConstantDesc) extracted[1];
        }

        return constantDescs;
    }

    private void processingInOperator(ExprNodeGenericFuncDesc expr, List<IndexSearchCondition>
            searchConditions, boolean isNot, Object... nodeOutputs) {
        ExprNodeDesc columnDesc;
        String[] fields = null;

        if (LOG.isTraceEnabled()) {
            LOG.trace("Processing In Operator. nodeOutputs : " + new ArrayList<>(Arrays.asList(nodeOutputs)));
        }

        columnDesc = (ExprNodeDesc) nodeOutputs[0];
        if (columnDesc instanceof ExprNodeFieldDesc) {
            // rowKey field
            ExprNodeFieldDesc fieldDesc = (ExprNodeFieldDesc) columnDesc;
            fields = ExprNodeDescUtils.extractFields(fieldDesc);

            ExprNodeDesc[] extracted = ExprNodeDescUtils.extractComparePair((ExprNodeDesc)
                    nodeOutputs[0], (ExprNodeDesc) nodeOutputs[1]);

            if (extracted == null) {    // adding for tez
                return;
            }

            if (LOG.isTraceEnabled()) {
                LOG.trace("nodeOutputs[0] : " + nodeOutputs[0] + ", nodeOutputs[1] : " +
                        nodeOutputs[1] + " => " + new ArrayList<>(Arrays.asList(extracted)));
            }

            columnDesc = extracted[0];
        }

        addSearchConditionIfPossible(expr, searchConditions, fields, isNot, columnDesc,
                Arrays.copyOfRange(nodeOutputs, 1, nodeOutputs.length));
    }

    private void processingNullOperator(ExprNodeGenericFuncDesc expr, List<IndexSearchCondition>
            searchConditions, Object... nodeOutputs) {
        ExprNodeDesc columnDesc = null;
        String[] fields = null;

        columnDesc = (ExprNodeDesc) nodeOutputs[0];
        if (columnDesc instanceof ExprNodeFieldDesc) {
            // rowKey field
            ExprNodeFieldDesc fieldDesc = (ExprNodeFieldDesc) columnDesc;
            fields = ExprNodeDescUtils.extractFields(fieldDesc);

            ExprNodeDesc[] extracted = ExprNodeDescUtils.extractComparePair(columnDesc,
                    new ExprNodeConstantDesc());
            columnDesc = extracted[0];
        }

        addSearchConditionIfPossible(expr, searchConditions, fields, false, columnDesc, null);
    }

    private void processingNotNullOperator(ExprNodeGenericFuncDesc expr,
                                           List<IndexSearchCondition> searchConditions, Object...
                                                   nodeOutputs) {
        ExprNodeDesc columnDesc;
        String[] fields = null;

        columnDesc = (ExprNodeDesc) nodeOutputs[0];
        if (columnDesc instanceof ExprNodeFieldDesc) {
            // rowKey field
            ExprNodeFieldDesc fieldDesc = (ExprNodeFieldDesc) columnDesc;
            fields = ExprNodeDescUtils.extractFields(fieldDesc);

            ExprNodeDesc[] extracted = ExprNodeDescUtils.extractComparePair(columnDesc,
                    new ExprNodeConstantDesc());
            columnDesc = extracted[0];
        }

        addSearchConditionIfPossible(expr, searchConditions, fields, true, columnDesc, null);
    }

    private ExprNodeDesc analyzeExpr(ExprNodeGenericFuncDesc expr, List<IndexSearchCondition>
            searchConditions, Object... nodeOutputs) throws SemanticException {

        if (FunctionRegistry.isOpAnd(expr)) {
            List<ExprNodeDesc> residuals = new ArrayList<>();
            // GenericUDFOPAnd can expect more than 2 arguments after HIVE-11398
            for (Object nodeOutput : nodeOutputs) {
                // The null value of nodeOutput means the predicate is pushed down to Phoenix. So
                // we don't need to add it to the residual predicate list
                if (nodeOutput != null) {
                    residuals.add((ExprNodeDesc) nodeOutput);
                }
            }
            if (residuals.size() == 1) {
                return residuals.get(0);
            }
            return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, FunctionRegistry
                    .getGenericUDFForAnd(), residuals);
        }

        GenericUDF genericUDF = expr.getGenericUDF();
        if (!(genericUDF instanceof GenericUDFBaseCompare)) {
            // 2015-10-22 Added by JeongMin Ju : Processing Between/In Operator
            if (genericUDF instanceof GenericUDFBetween) {
                // In case of not between, The value of first element of nodeOutputs is true.
                // otherwise false.
                processingBetweenOperator(expr, searchConditions, nodeOutputs);
                return expr;
            } else if (genericUDF instanceof GenericUDFIn) {
                // In case of not in operator, in operator exist as child of not operator.
                processingInOperator(expr, searchConditions, false, nodeOutputs);
                return expr;
            } else if (genericUDF instanceof GenericUDFOPNot &&
                    ((ExprNodeGenericFuncDesc) expr.getChildren().get(0)).getGenericUDF()
                            instanceof GenericUDFIn) {
                // In case of not in operator, in operator exist as child of not operator.
                processingInOperator((ExprNodeGenericFuncDesc) expr.getChildren().get(0),
                        searchConditions, true, ((ExprNodeGenericFuncDesc) nodeOutputs[0])
                                .getChildren().toArray());
                return expr;
            } else if (genericUDF instanceof GenericUDFOPNull) {
                processingNullOperator(expr, searchConditions, nodeOutputs);
                return expr;
            } else if (genericUDF instanceof GenericUDFOPNotNull) {
                processingNotNullOperator(expr, searchConditions, nodeOutputs);
                return expr;
            } else {
                return expr;
            }
        }
        ExprNodeDesc expr1 = (ExprNodeDesc) nodeOutputs[0];
        ExprNodeDesc expr2 = (ExprNodeDesc) nodeOutputs[1];
        // We may need to peel off the GenericUDFBridge that is added by CBO or
        // user
        if (expr1.getTypeInfo().equals(expr2.getTypeInfo())) {
            expr1 = getColumnExpr(expr1);
            expr2 = getColumnExpr(expr2);
        }

        ExprNodeDesc[] extracted = ExprNodeDescUtils.extractComparePair(expr1, expr2);
        if (extracted == null || (extracted.length > 2 && !acceptsFields)) {
            return expr;
        }

        ExprNodeColumnDesc columnDesc;
        ExprNodeConstantDesc constantDesc;
        if (extracted[0] instanceof ExprNodeConstantDesc) {
            genericUDF = genericUDF.flip();
            columnDesc = (ExprNodeColumnDesc) extracted[1];
            constantDesc = (ExprNodeConstantDesc) extracted[0];
        } else {
            columnDesc = (ExprNodeColumnDesc) extracted[0];
            constantDesc = (ExprNodeConstantDesc) extracted[1];
        }

        Set<String> allowed = columnToUDFs.get(columnDesc.getColumn());
        if (allowed == null) {
            return expr;
        }

        String udfName = genericUDF.getUdfName();
        if (!allowed.contains(genericUDF.getUdfName())) {
            return expr;
        }

        String[] fields = null;
        if (extracted.length > 2) {
            ExprNodeFieldDesc fieldDesc = (ExprNodeFieldDesc) extracted[2];
            if (!isValidField(fieldDesc)) {
                return expr;
            }
            fields = ExprNodeDescUtils.extractFields(fieldDesc);
        }

        // We also need to update the expr so that the index query can be
        // generated.
        // Note that, hive does not support UDFToDouble etc in the query text.
        List<ExprNodeDesc> list = new ArrayList<ExprNodeDesc>();
        list.add(expr1);
        list.add(expr2);
        expr = new ExprNodeGenericFuncDesc(expr.getTypeInfo(), expr.getGenericUDF(), list);

        searchConditions.add(new IndexSearchCondition(columnDesc, udfName, constantDesc, expr,
                fields));

        // we converted the expression to a search condition, so
        // remove it from the residual predicate
        return fields == null ? null : expr;
    }

    private boolean isValidField(ExprNodeFieldDesc field) {
        return fieldValidator == null || fieldValidator.validate(field);
    }

    /**
     * Translates search conditions back to ExprNodeDesc form (as a left-deep
     * conjunction).
     *
     * @param searchConditions (typically produced by analyzePredicate)
     * @return ExprNodeGenericFuncDesc form of search conditions
     */
    public ExprNodeGenericFuncDesc translateSearchConditions(List<IndexSearchCondition>
                                                                     searchConditions) {

        ExprNodeGenericFuncDesc expr = null;

        for (IndexSearchCondition searchCondition : searchConditions) {
            if (expr == null) {
                expr = searchCondition.getComparisonExpr();
                continue;
            }

            List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
            children.add(expr);
            children.add(searchCondition.getComparisonExpr());
            expr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, FunctionRegistry
                    .getGenericUDFForAnd(), children);
        }

        return expr;
    }

    public void setAcceptsFields(boolean acceptsFields) {
        this.acceptsFields = acceptsFields;
    }

    public static interface FieldValidator {
        boolean validate(ExprNodeFieldDesc exprNodeDesc);
    }

    public static IndexPredicateAnalyzer createAnalyzer(boolean equalOnly) {
        IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");

        if (equalOnly) {
            return analyzer;
        }

        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic" +
                ".GenericUDFOPEqualOrGreaterThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic" +
                ".GenericUDFOPEqualOrLessThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");

        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual");
        // apply !=
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween");
        // apply (Not) Between
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn");        //
        // apply (Not) In
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn");        //
        // apply In
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull");
        // apply Null
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull");
        // apply Not Null

        return analyzer;
    }
}
