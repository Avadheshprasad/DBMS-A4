package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import java.util.List;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;


public class PFilter extends Filter implements PRel {

    private final PRel childs;
    public PFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RexNode condition) {
        super(cluster, traits, child, condition);
        assert getConvention() instanceof PConvention;
        this.childs = (child instanceof PRel) ? (PRel) child: null;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PFilter");
        /* Write your code here */
        // return false;
        return childs.open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        /* Write your code here */
        childs.close();
        return;
    }
    private Object[] temp = null;
    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PFilter has next");
        /* Write your code here */
        if(temp !=null){
            return true;
        }
        while (childs.hasNext()) {
            Object[] data = childs.next();
            if (check(condition, data)) {
                temp = data;
                return true;
            }
        }
        return false;
    }

    // returns the next row
    // Hint: Try looking at different possible filter conditions
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PFilter");
        /* Write your code here */
        Object[] row = temp;
        temp = null; 
        return row;
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

    private boolean check(RexNode condition, Object[] row) {
        RexCall call = (RexCall) condition;
        List<RexNode> operands = call.getOperands();
        if ("AND".equals(call.getOperator().getKind().toString())) {
            return check(operands.get(0), row) && check(operands.get(1), row);
        } else if ("OR".equals(call.getOperator().getKind().toString())) {
            return check(operands.get(0), row) || check(operands.get(1), row);
        } else{
            Object left = eval(operands.get(0), row);
            Object right = eval(operands.get(1), row);
            if (left == null || right == null) {
                    return false; 
                }
            if ("EQUALS".equals(call.getOperator().getKind().toString())) {
                return check_operator(left, right, "EQUALS"); 
            } else if ("NOT_EQUALS".equals(call.getOperator().getKind().toString())) {
                return !check_operator(left, right, "EQUALS");
            } else if ("GREATER_THAN".equals(call.getOperator().getKind().toString())) {
                return check_operator(left, right, "GREATER");
            } else if ("GREATER_THAN_OR_EQUAL".equals(call.getOperator().getKind().toString())) {
                return check_operator(left, right, "GREATER_OR_EQUAL");
            } else if ("LESS_THAN".equals(call.getOperator().getKind().toString())) {
                return check_operator(left, right, "LESS");
            } else if ("LESS_THAN_OR_EQUAL".equals(call.getOperator().getKind().toString())) {
                return check_operator(left, right, "LESS_OR_EQUAL");
            } else {
                return true;
            }
        } 
    }

    private boolean check_operator(Object left, Object right, String relation) {
        int is_relation;
        if (left instanceof Number && right instanceof Number) {
            double a = ((Number)left).doubleValue();
            double b = ((Number)right).doubleValue();
            is_relation = Double.compare(a,b);
        }
        else if (left instanceof Comparable && right instanceof Comparable) {
            is_relation = ((Comparable) left).compareTo(right);
        }
        else {
            is_relation = left.toString().compareTo(right.toString());
        }

        if (relation.equals("EQUALS")) {
            return is_relation == 0;
        } else if (relation.equals("GREATER")) {
            return is_relation > 0;
        } else if (relation.equals("LESS")) {
            return is_relation < 0;
        } else if (relation.equals("LESS_OR_EQUAL")) {
            return is_relation <= 0;
        } else {
            return is_relation >= 0;
        }
    }

}