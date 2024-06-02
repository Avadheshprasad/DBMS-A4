package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import convention.PConvention;

import java.util.List;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {

    private final PRel childs;
    
    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
        this.childs = (input instanceof PRel) ? (PRel) input: null;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    private List<Object[]> results;
    private int currentIndex;
    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PAggregate");
        childs.open();
        Map<List<Object>, Object[]> groupedResults = new HashMap<>();

        while (childs.hasNext()) {
            Object[] inputRow = childs.next(); 
            List<Object> groupKey = new ArrayList<>();
            for (int column : groupSet) {
                groupKey.add(inputRow[column]);
            }
            
            groupedResults.compute(groupKey, (k, v) -> {
                if (v == null) {
                    Object[] aggregatedRow = new Object[aggCalls.size()+1];
                    for (int i = 0; i < aggCalls.size(); i++) {
                        AggregateCall aggCall = aggCalls.get(i);
                        if (aggCall.getAggregation().getName().equals("COUNT")) {
                            aggregatedRow[i] = 1;
                            aggregatedRow[aggCalls.size()] = 1;
                        } else {
                            aggregatedRow[i] = inputRow[aggCall.getArgList().get(0)];
                            aggregatedRow[aggCalls.size()] = 1;
                        }
                    }
                    return aggregatedRow;
                } else {
                    for (int i = 0; i < aggCalls.size(); i++) {
                        double old,newValue;
                        AggregateCall aggCall = aggCalls.get(i);
                        switch (aggCall.getAggregation().getName()) {
                            case "COUNT":
                                int count = (int) v[i];
                                count++;
                                v[i] = count;
                                break;
                            case "MAX":
                                old = ((Number) v[i]).doubleValue();
                                newValue = ((Number) inputRow[aggCall.getArgList().get(0)]).doubleValue();
                                v[i] = Math.max(old, newValue);
                                break;
                            case "MIN":
                                old = ((Number) v[i]).doubleValue();
                                newValue = ((Number) inputRow[aggCall.getArgList().get(0)]).doubleValue();
                                v[i] = Math.min(old, newValue);
                                break;
                            case "SUM":
                                v[i] = ((Number) v[i]).doubleValue() + ((Number) inputRow[aggCall.getArgList().get(0)]).doubleValue();
                                break;
                            case "AVG":
                                int tt= (int) v[aggCalls.size()];
                                tt++;
                                v[aggCalls.size()]=tt;
                                v[i] = ((Number) v[i]).doubleValue() + ((Number) inputRow[aggCall.getArgList().get(0)]).doubleValue();
                                break;
                        }
                    }
                    return v;
                }
            });
        }
        results = new ArrayList<>();
        for (Map.Entry<List<Object>, Object[]> entry : groupedResults.entrySet()) {
            List<Object> key = entry.getKey();
            Object[] value = entry.getValue();
            Object[] tem= new Object[key.size()+value.length];
            for(int i=0;i<key.size();i++){
                tem[i]=key.get(i);
            }
            for(int i=0;i<aggCalls.size();i++){
                AggregateCall aggCall = aggCalls.get(i);
                if(aggCall.getAggregation().getName().equals("AVG")){
                    tem[i+key.size()]= ((Number) value[i]).doubleValue()/((Number) value[aggCalls.size()]).doubleValue();
                }
                else{
                    tem[i+key.size()]=value[i];
                }
                
            }
            
            results.add(tem);
        }
        
        currentIndex = 0;
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        /* Write your code here */
        childs.close();
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        return currentIndex < results.size();
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        return results.get(currentIndex++);
    }
   
}