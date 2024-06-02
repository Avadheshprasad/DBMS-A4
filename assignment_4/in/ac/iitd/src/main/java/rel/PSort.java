package rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.calcite.rel.RelFieldCollation;
import convention.PConvention;

public class PSort extends Sort implements PRel{
    private final PRel childs;
    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
            ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
        this.childs = (child instanceof PRel) ? (PRel) child: null;
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }

    private Iterator<Object[]> sortedRowsIterator;
    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        /* Write your code here */
        // return false;
        childs.open();
        List<Object[]> rows = new ArrayList<>();
        while (childs.hasNext()) {
            rows.add(childs.next());
        }
    
        Comparator<Object[]> comparator = createComparator(collation);
        if (comparator != null) {
            Collections.sort(rows, comparator);
        }

        int offsetRows = offset != null ?  Integer.parseInt(offset.toString()) : 0;
        int fetchRows = fetch != null ? Integer.parseInt(fetch.toString()) : Integer.MAX_VALUE;
        if (offsetRows > 0 || fetchRows < Integer.MAX_VALUE) {
            int numRows = rows.size();
            int endIndex = Math.min(numRows, offsetRows + fetchRows);
            rows = rows.subList(Math.min(numRows, offsetRows), endIndex);
        }
        sortedRowsIterator = rows.iterator();
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        /* Write your code here */
        // return;
        childs.close();
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        /* Write your code here */
        // return false;
        return sortedRowsIterator.hasNext();
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        /* Write your code here */
        // return null;
        return sortedRowsIterator.next();
    }


private Comparator<Object[]> createComparator(RelCollation collation) {
        if (collation == null || collation.getFieldCollations().isEmpty()) {
            return null;
        }

        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        List<Comparator<Object[]>> comparators = new ArrayList<>(fieldCollations.size());

        for (RelFieldCollation fieldCollation : fieldCollations) {
            int fieldIndex = fieldCollation.getFieldIndex();
            Comparator<Object[]> fieldComparator = new ObjectComparator(fieldIndex);
            comparators.add(fieldCollation.getDirection().isDescending() ? Collections.reverseOrder(fieldComparator) : fieldComparator);
        }

        Comparator<Object[]> combinedComparator = comparators.get(0);
        for (int i = 1; i < comparators.size(); i++) {
            combinedComparator = combinedComparator.thenComparing(comparators.get(i));
        }

        return combinedComparator;
    }

    private static class ObjectComparator implements Comparator<Object[]> {
        private final int fieldIndex;

        public ObjectComparator(int fieldIndex) {
            this.fieldIndex = fieldIndex;
        }

        @Override
        public int compare(Object[] row1, Object[] row2) {
            Comparable<Object> val1 = (Comparable<Object>) row1[fieldIndex];
            Comparable<Object> val2 = (Comparable<Object>) row2[fieldIndex];
            return val1.compareTo(val2);
        }
    }

}
