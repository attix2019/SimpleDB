package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;

    private Type gbfieldType;

    private int afield;

    private Op what;

    private Map<Field, Integer> result;

    private static final Field PLACEHOLDER = new IntField(0);

    private Field[] traverseHelper;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if(what != Op.COUNT){
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        result = new HashMap();
        if(gbfield == NO_GROUPING){
            result.put(PLACEHOLDER,0);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(gbfield == NO_GROUPING){
            int oldValue = result.get(PLACEHOLDER);
            result.put(PLACEHOLDER, oldValue + 1);
            return;
        }
        Field gbfieldValue =  tup.getField(gbfield);
        if(result.containsKey(gbfieldValue)){
            int oldValue = result.get(gbfieldValue);
            result.put(gbfieldValue, oldValue + 1);
        }else{
            result.put(gbfieldValue, 1);
            traverseHelper = result.keySet().toArray(new Field[0]);
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
        return new OpIterator() {

            boolean isOpen = false;
            int i = 0;
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
                return i < traverseHelper.length;
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
                Tuple ans = new Tuple(td);
                if(gbfield == NO_GROUPING){
                    ans.setField(0, new IntField(aggregateValue) );
                }else{
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
                return td;
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
