package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.awt.geom.RectangularShape;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;

    private int afield;

    private Type gbfieldType;

    Aggregator.Op what;

    private Map<Field, Integer> result;

    private Map<Field, Integer> avgHelper;

    private Field[] traverseHelper;

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
        this.afield = afield;
        this.gbfieldType = gbfieldtype;
        this.gbfield = gbfield;
        this.what = what;
        result = new HashMap();
        if(what == Op.AVG){
            avgHelper = new HashMap();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(gbfield == NO_GROUPING){

        }
        Field gbfieldValue =  tup.getField(gbfield);
        IntField aFieldValue = (IntField)tup.getField(afield);
        if(result.containsKey(gbfieldValue)){
            int oldValue = result.get(gbfieldValue);
            if(what == Op.COUNT){
                result.put(gbfieldValue, oldValue + 1);
            }else if(what == Op.SUM){
                result.put(gbfieldValue, oldValue + aFieldValue.getValue());
            }else if(what == Op.AVG){
                int oldCount = avgHelper.get(gbfieldValue);
                result.put(gbfieldValue, (oldValue * oldCount + aFieldValue.getValue())/(oldCount + 1));
                avgHelper.put(gbfieldValue, oldCount + 1);
            }else if(what == Op.MIN){
                result.put(gbfieldValue, Math.min(oldValue, aFieldValue.getValue()));
            }else if(what == Op.MAX){
                result.put(gbfieldValue, Math.max(oldValue, aFieldValue.getValue()));
            }
        }else{
            if(what == Op.COUNT){
                result.put(gbfieldValue, 1);
            }else if(what == Op.SUM){
                result.put(gbfieldValue, aFieldValue.getValue());
            }else if(what == Op.AVG){
                result.put(gbfieldValue,  aFieldValue.getValue());
                avgHelper.put(gbfieldValue, 1);
            }else if(what == Op.MIN){
                result.put(gbfieldValue, aFieldValue.getValue());
            }else if(what == Op.MAX){
                result.put(gbfieldValue, aFieldValue.getValue());
            }
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
        traverseHelper = result.keySet().toArray(new Field[0]);

        return new OpIterator() {
            int i = 0;
            boolean isOpen = false;
            TupleDesc td = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE});

            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!isOpen){
                    throw new IllegalStateException();
                }
                return i < result.size();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!isOpen){
                    throw new IllegalStateException();
                }
                if(i >= traverseHelper.length){
                    throw new NoSuchElementException();
                }
                int aggregateValue = result.get(traverseHelper[i]);
                Tuple ans = null;
                if(gbfield == NO_GROUPING){

                }else{
                    ans = new Tuple(td);
                    ans.setField(0, traverseHelper[i] );
                    ans.setField(1,  new IntField(aggregateValue));
                }
                i++;
                return ans;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if(!isOpen){
                    throw new IllegalStateException();
                }
                i = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return null;
            }

            @Override
            public void close() {
                if(!isOpen){
                    throw new IllegalStateException();
                }
                isOpen = false;
            }
        };
    }

}
