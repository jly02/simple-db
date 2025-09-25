package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // Key to use when gbfield == NO_GROUPING
    private static final Field NG_KEY = null;

    /** Internal information */
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    /** Store the groups, or average of a group for avg */
    private final Map<Field, Integer> groups;
    private final Map<Field, int[]> avgs;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.groups = new HashMap<>();
        this.avgs = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbkey;
        if (gbfield == NO_GROUPING) {
            gbkey = NG_KEY;
        } else {
            gbkey = tup.getField(this.gbfield);
        }

        int tval = ((IntField) tup.getField(this.afield)).getValue();
        switch (this.what) {
            case COUNT:
                groups.put(gbkey, groups.getOrDefault(gbkey, 0) + 1);
                break;
            
            case SUM:
                groups.put(gbkey, tval + groups.getOrDefault(gbkey, 0));
                break;

            case AVG:
                int sum = avgs.getOrDefault(gbkey, new int[]{0, 0})[0];
                int cnt = avgs.getOrDefault(gbkey, new int[]{0, 0})[1];
                avgs.put(gbkey, new int[]{sum + tval, cnt + 1});
                break;
            
            case MIN:
                int minv = groups.getOrDefault(gbkey, Integer.MAX_VALUE);
                groups.put(gbkey, Math.min(tval, minv));
                break;

            case MAX:
                int maxv = groups.getOrDefault(gbkey, Integer.MIN_VALUE);
                groups.put(gbkey, Math.max(tval, maxv));
                break;
        
            default:
                throw new RuntimeException("IntegerAggregator: ???");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new IntegerAggregatorIterator();
    }

    public class IntegerAggregatorIterator implements OpIterator {
        /** Internal information */
        private final TupleDesc td;
        private List<Tuple> tuples;
        private Iterator<Tuple> it;

        public IntegerAggregatorIterator() {
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
                Tuple t = new Tuple(td);
                if (what == Op.AVG) {
                    int[] v = avgs.get(NG_KEY);
                    int avg = v[0] / v[1];
                    t.setField(0, new IntField(avg));
                } else {
                    t.setField(0, new IntField(groups.get(NG_KEY)));
                }

                tuples.add(t);
            } else {
                // yes grouping
                if (what == Op.AVG) {
                    for (Field f : avgs.keySet()) {
                        Tuple t = new Tuple(td);
                        t.setField(0, f);
                        int[] v = avgs.get(f);
                        t.setField(1, new IntField(v[0] / v[1]));
                        tuples.add(t);
                    }
                } else {
                    for (Field f : groups.keySet()) {
                        Tuple t = new Tuple(td);
                        t.setField(0, f);
                        int v = groups.get(f);
                        t.setField(1, new IntField(v));
                        tuples.add(t);
                    }
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
