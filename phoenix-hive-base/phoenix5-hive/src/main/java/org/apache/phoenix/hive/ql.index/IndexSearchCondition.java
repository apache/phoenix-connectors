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

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.Arrays;

/**
 * IndexSearchCondition represents an individual search condition found by
 * {@link IndexPredicateAnalyzer}.
 *
 */
public class IndexSearchCondition extends IndexSearchConditionBase{

    public IndexSearchCondition(ExprNodeColumnDesc columnDesc, String comparisonOp, ExprNodeConstantDesc[] multiConstants, ExprNodeGenericFuncDesc comparisonExpr, boolean isNot) {
        super(columnDesc, comparisonOp, multiConstants, comparisonExpr, isNot);
    }

    public IndexSearchCondition(ExprNodeColumnDesc columnDesc, String comparisonOp, ExprNodeConstantDesc[] multiConstants, ExprNodeGenericFuncDesc comparisonExpr, String[] fields, boolean isNot) {
        super(columnDesc, comparisonOp, multiConstants, comparisonExpr, fields, isNot);
    }

    public IndexSearchCondition(ExprNodeColumnDesc columnDesc, String comparisonOp, ExprNodeConstantDesc constantDesc, ExprNodeGenericFuncDesc comparisonExpr) {
        super(columnDesc, comparisonOp, constantDesc, comparisonExpr);
    }

    public IndexSearchCondition(ExprNodeColumnDesc columnDesc, String comparisonOp, ExprNodeConstantDesc constantDesc, ExprNodeGenericFuncDesc comparisonExpr, String[] fields) {
        super(columnDesc, comparisonOp, constantDesc, comparisonExpr, fields);
    }

    public ExprNodeGenericFuncDesc getComparisonExpr() {
        ExprNodeGenericFuncDesc ret = comparisonExpr;
        try {
            if (GenericUDFIn.class == comparisonExpr.getGenericUDF().getClass() && isNot) {
                ret = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                        FunctionRegistry.getFunctionInfo("not").getGenericUDF(),
                        Arrays.asList(comparisonExpr));
            }
        } catch (SemanticException e) {
            throw new RuntimeException("hive operator -- never be thrown", e);
        }
        return ret;
    }
}
