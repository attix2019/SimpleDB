package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.awt.geom.RectangularShape;
import java.util.*;

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

    private Map<Field, Integer> avgSum;

    private Map<Field, Integer> avgCount;

//    private Field[] traverseHelper;

    private List<Field> keyList = new LinkedList();

    private static final Field PLACEHOLDER = new IntField(0);

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
            avgSum = new HashMap();
            avgCount = new HashMap();
        }
        if(gbfield == NO_GROUPING){
            keyList.add(PLACEHOLDER);
            result.put(PLACEHOLDER,0);
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
        Field gbfieldValue;
        IntField aFieldValue = (IntField)tup.getField(afield);
        if(gbfield == NO_GROUPING){
            int oldValue = result.get(PLACEHOLDER);
            if(what == Op.COUNT){
                result.put(PLACEHOLDER, oldValue + 1);
            }else if(what == Op.SUM){
                result.put(PLACEHOLDER, oldValue + aFieldValue.getValue());
            }else if(what == Op.AVG){
                int newSum = avgSum.getOrDefault(PLACEHOLDER, 0) + aFieldValue.getValue();
                avgSum.put(PLACEHOLDER, newSum);
                int newCount = avgCount.getOrDefault(PLACEHOLDER, 0) +1;
                avgCount.put(PLACEHOLDER, newCount);
                result.put(PLACEHOLDER, newSum/newCount );
            }else if(what == Op.MIN){
                result.put(PLACEHOLDER, Math.min(oldValue, aFieldValue.getValue()));
            }else if(what == Op.MAX){
                result.put(PLACEHOLDER, Math.max(oldValue, aFieldValue.getValue()));
            }
            return;
        }
        gbfieldValue =  tup.getField(gbfield);
        if(result.containsKey(gbfieldValue)){
            int oldValue = result.get(gbfieldValue);
            if(what == Op.COUNT){
                result.put(gbfieldValue, oldValue + 1);
            }else if(what == Op.SUM){
                result.put(gbfieldValue, oldValue + aFieldValue.getValue());
            }else if(what == Op.AVG){
                int newSum = avgSum.getOrDefault(gbfieldValue, 0) + aFieldValue.getValue();
                avgSum.put(gbfieldValue, newSum);
                int newCount = avgCount.getOrDefault(gbfieldValue, 0) +1;
                avgCount.put(gbfieldValue, newCount);
                result.put(gbfieldValue, newSum/newCount );
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
                avgSum.put(gbfieldValue, aFieldValue.getValue());
                avgCount.put(gbfieldValue, 1);
            }else if(what == Op.MIN){
                result.put(gbfieldValue, aFieldValue.getValue());
            }else if(what == Op.MAX){
                result.put(gbfieldValue, aFieldValue.getValue());
            }
            keyList.add(gbfieldValue);
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

        return new OpIterator() {
            int i = 0;
            boolean isOpen = false;
            TupleDesc td = (gbfield == NO_GROUPING)?
                new TupleDesc(new Type[]{Type.INT_TYPE})
                :new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE});

            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!isOpen){
                    throw new IllegalStateException();
                }
                return i < keyList.size();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!isOpen){
                    throw new IllegalStateException();
                }
                if(i >= keyList.size()){
                    throw new NoSuchElementException();
                }
                int aggregateValue = result.get(keyList.get(i));
                Tuple ans = null;
                ans = new Tuple(td);
                if(gbfield == NO_GROUPING){
                    ans.setField(0, new IntField(aggregateValue) );
                }else{
                    ans.setField(0, keyList.get(i) );
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
                return td;
            }

            @Override
            public void close() {
                if(!isOpen){
                    throw new IllegalStateException();
                }
                avgSum = null;
                avgCount = null;
                result = null;
                isOpen = false;
            }
        };
    }

}
