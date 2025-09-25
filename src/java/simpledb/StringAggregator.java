package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // Key to use when gbfield == NO_GROUPING
    private static final Field NG_KEY = null;

    /** Internal information */
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    /** Store the groups */
    private final Map<Field, Integer> groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator: what");   
        }

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbkey;
        if (gbfield == NO_GROUPING) {
            gbkey = NG_KEY;
        } else {
            gbkey = tup.getField(gbfield);
        }

        switch (what) {
            case COUNT:
                groups.put(gbkey, groups.getOrDefault(gbkey, 0) + 1);
                break;

            default:
                throw new RuntimeException("something went wrong");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new StringAggregatorIterator();
    }

    public class StringAggregatorIterator implements OpIterator {
        /** Internal information */
        private final TupleDesc td;
        private List<Tuple> tuples;
        private Iterator<Tuple> it;

        public StringAggregatorIterator() {
            if (gbfield == NO_GROUPING) {
                this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
            } else {
                this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.tuples = new ArrayList<>();
            if (gbfield == NO_GROUPING) {
                // no grouping
                Tuple tup = new Tuple(td);
                tup.setField(0, new IntField(groups.get(NG_KEY)));
                tuples.add(tup);
            } else {
                // yes grouping
                for (Field gbKey : groups.keySet()) {
                    Tuple tup = new Tuple(td);
                    tup.setField(0, gbKey);
                    tup.setField(1, new IntField(groups.get(gbKey)));
                    tuples.add(tup);
                }
            }

            this.it = tuples.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return this.it != null && it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.it = tuples.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return this.td;
        }

        @Override
        public void close() {
            this.it = null;
        }

    }

}
