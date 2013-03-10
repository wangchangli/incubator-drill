package org.apache.drill.adhoc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.OperatorGraph;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.LogicalOperator;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.ConsoleRSE;
import org.apache.drill.exec.ref.rse.RSERegistry;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-20
 * Time: 上午11:20
 * To change this template use File | Settings | File Templates.
 */
public class PlanParser {
    private CCJSqlParserManager pm = new CCJSqlParserManager();
    private static PlanParser instance = new PlanParser();

    private PlanParser() {

    }

    public static PlanParser getInstance() {
        return instance;
    }

    public LogicalPlan parse(String sql) throws JSQLParserException, Exception {
        LogicalPlan plan = null;
        Statement statement = pm.parse(new StringReader(sql));
        if (statement instanceof Select) {
            Select selectStatement  = (Select) statement;
            AdhocSQLQueryVisitorImpl visitor = new AdhocSQLQueryVisitorImpl();
            selectStatement.getSelectBody().accept(visitor);
            List<LogicalOperator> logicalOperators = visitor.getLogicalOperators();

            ObjectMapper mapper = new ObjectMapper();
            PlanProperties head = mapper.readValue(new String("{\"type\":\"apache_drill_logical_plan\",\"version\":\"1\",\"generator\":{\"type\":\"manual\",\"info\":\"na\"}}").getBytes(), PlanProperties.class);

            //List<StorageEngineConfig> storageEngines = mapper.readValue(new String("[{\"type\":\"console\",\"name\":\"console\"},{\"type\":\"fs\",\"name\":\"fs\",\"root\":\"file:///\"}]").getBytes(),new TypeReference<List<StorageEngineConfig>>() {});
            List<StorageEngineConfig> storageEngines = new ArrayList<>();
            storageEngines.add(mapper.readValue(new String("{\"type\":\"console\",\"name\":\"console\"}").getBytes(),ConsoleRSE.ConsoleRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"fs\",\"name\":\"fs\",\"root\":\"file:///\"}").getBytes(),ConsoleRSE.ConsoleRSEConfig.class));

            plan = new LogicalPlan(head, storageEngines, logicalOperators);
        }
        return plan;
    }

    public static void main(String[] args) throws Exception{
        DrillConfig config = DrillConfig.create();
        LogicalPlan logicalPlan = new PlanParser().parse("Select count(event.uid) FROM user INNER JOIN event ON user.uid=event.uid WHERE user.register_time>=20130101000000 and user.register_time<20130102000000 and event.l0='visit' and event.date='2013-01-02'\n");
        IteratorRegistry ir = new IteratorRegistry();
        ReferenceInterpreter i = new ReferenceInterpreter(logicalPlan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
        i.setup();
        Collection<RunOutcome> outcomes = i.run();

    }
}
