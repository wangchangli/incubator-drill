package org.apache.drill.adhoc;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.JSONOptions;
import org.apache.drill.common.logical.data.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.drill.exec.ref.rse.FileSystemRSE;
import org.apache.drill.exec.ref.rse.FileSystemRSE.HBaseEntry;
import org.apache.drill.exec.ref.rse.FileSystemRSE.MySqlEntry;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-20
 * Time: 上午11:45
 * To change this template use File | Settings | File Templates.
 */
public class AdhocFromItemVisitorImpl implements FromItemVisitor {

    private LogicalOperator lop;


    @Override
    public void visit(Table table) {
        String tableName = table.getName();
        FieldReference fr = new FieldReference(tableName);
        String storageEngine = "fs1";
        JSONOptions selection =  null;
        ObjectMapper mapper = new ObjectMapper();

        try{
            if (tableName.contains("deu")){
                //HBaseEntry hBaseEntry = new HBaseEntry("start","end");
                selection = mapper.readValue(new String("{startKey:\"xxx\",endKey:\"yyy\"}").getBytes(), JSONOptions.class);
            }else{
                //MySqlEntry mySqlEntry = new MySqlEntry("s","e");
                selection = mapper.readValue(new String("{startKey:\"xxx\",endKey:\"yyy\"}").getBytes(), JSONOptions.class);
            }
        }catch (Exception e){

        }

        lop = new Scan(storageEngine,selection,fr);

    }

    @Override
    public void visit(SubSelect subSelect) {
        AdhocSQLQueryVisitorImpl visitor = new AdhocSQLQueryVisitorImpl();
        subSelect.getSelectBody().accept(visitor);
        List<LogicalOperator> lops = visitor.getLogicalOperators();
        for (LogicalOperator lopTmp : lops) {
            System.out.println(lopTmp);
        }
    }

    @Override
    public void visit(SubJoin subJoin) {
        FromItem left = subJoin.getLeft();
        FromItem right = subJoin.getJoin().getRightItem();
        AdhocFromItemVisitorImpl lv = new AdhocFromItemVisitorImpl();
        left.accept(lv);
        AdhocFromItemVisitorImpl rv = new AdhocFromItemVisitorImpl();
        right.accept(rv);

        LogicalOperator leftLop = lv.getLogicalOperator();
        LogicalOperator rightLop = rv.getLogicalOperator();

        Expression expr = subJoin.getJoin().getOnExpression();
        JoinCondition[] jcs = null;
        if (expr != null) {
            AdhocExpressionVisitorImpl exprVisitor = new AdhocExpressionVisitorImpl();
            expr.accept(exprVisitor);
            FunctionCall funcExpr = (FunctionCall) exprVisitor.getLogicalExpression();
            List<LogicalExpression> args = funcExpr.args;
            String relationship = getRelationship(funcExpr.getDefinition().getName());
            JoinCondition jc = new JoinCondition(relationship, args.get(0), args.get(1));
            jcs = new JoinCondition[1];
            jcs[0] = jc;
        } else {
            jcs = new JoinCondition[0];
        }
        lop = new Join(leftLop, rightLop, jcs, returnJoinType(subJoin));
    }

    public LogicalOperator getLogicalOperator() {
        return lop;
    }

    private String returnJoinType(SubJoin subJoin) {
        if (subJoin.getJoin().isInner()) {
            return "INNER";
        } else if (subJoin.getJoin().isNatural()) {
            return "OUTER";
        } else if (subJoin.getJoin().isOuter()) {
            return "OUTER";
        } else if (subJoin.getJoin().isLeft()) {
            return "LEFT";
        }
        //throw new AdhocRuntimeException("Can't parse join type of " + subJoin.toString());
        //throw new Exception(); //wcl
        return "";
    }

    private String getRelationship(String name) {
        if (name.equals("equal")) {
            return "==";
        }
        //throw new AdhocRuntimeException("Can't parse join condition relationship " + name);
        return "";
    }
}
