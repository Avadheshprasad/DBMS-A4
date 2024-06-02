package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.rex.RexInterpreter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {
    private final PRel childs;
    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
        this.childs = (input instanceof PRel) ? (PRel) input: null;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                            List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        /* Write your code here */
        // return false;
        return childs.open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        /* Write your code here */
        childs.close(); 
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
        return childs.hasNext();
        // return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        Object[] inputRow = childs.next();

        if (inputRow == null) {
            return null;
        }
        Object[] projectedRow = new Object[getRowType().getFieldCount()];

        List<RexNode> projectExpressions = getProjects();
        for (int i = 0; i < projectExpressions.size(); i++) {
            Object a1= eval(projectExpressions.get(i), inputRow);
            projectedRow[i] = a1;
        }

        return projectedRow;
    }


    private Object eval(RexNode exp, Object[] row) {
        if (exp instanceof RexCall) {
            RexCall call= (RexCall) exp;
            Object left = eval(call.operands.get(0), row);
            Object right = eval(call.operands.get(1), row);
            switch (call.getOperator().getKind()) {
                case PLUS:
                    return ((Number)left).doubleValue() + ((Number)right).doubleValue();
                case MINUS:
                    return ((Number)left).doubleValue() - ((Number)right).doubleValue();
                case TIMES:
                    return ((Number)left).doubleValue() * ((Number)right).doubleValue();
                case DIVIDE:
                    return ((Number)left).doubleValue() / ((Number)right).doubleValue();
                default:
                    return null;
            }
        }
        if (exp instanceof RexLiteral) {
            return ((RexLiteral) exp).getValue2();
        }
        return row[((RexInputRef) exp).getIndex()];
    }

}