package org.apache.phoenix.spark;

import org.apache.phoenix.end2end.BaseOrderByIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.QueryBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(ParallelStatsDisabledTest.class)
public class OrderByReverseOptimizationIT extends BaseOrderByIT {
    @Override
    protected ResultSet executeQueryThrowsException(Connection conn, QueryBuilder queryBuilder,
            String expectedPhoenixExceptionMsg, String expectedSparkExceptionMsg) {
        ResultSet rs = null;
        try {
            rs = executeQuery(conn, queryBuilder);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(expectedSparkExceptionMsg));
        }
        return rs;
    }

    @Override
    protected ResultSet executeQuery(Connection conn, QueryBuilder queryBuilder)
            throws SQLException {
        return SparkUtil.executeQuery(conn, queryBuilder, getUrl(), config);
    }

    @Test
    @Ignore
    public void testMultiOrderByExpr() throws Exception {
        super.testMultiOrderByExpr();
    }

    @Test
    @Ignore
    public void testDescMultiOrderByExpr() throws Exception {
        super.testDescMultiOrderByExpr();
    }

    @Test
    @Ignore
    public void testOrderByDifferentColumns() throws Exception {
        super.testOrderByDifferentColumns();
    }

    @Test
    @Ignore
    public void testAggregateOrderBy() throws Exception {
        super.testAggregateOrderBy();
    }

    @Test
    @Ignore
    public void testAggregateOptimizedOutOrderBy() throws Exception {
        super.testAggregateOptimizedOutOrderBy();
    }

    @Test
    @Ignore
    public void testNullsLastWithDesc() throws Exception {
        super.testNullsLastWithDesc();
    }

    @Test
    @Ignore
    public void testPhoenix6999() throws Exception {
        super.testPhoenix6999();
    }

    @Test
    @Ignore
    public void testOrderByNullable() throws SQLException {
        super.testOrderByNullable();
    }

}
