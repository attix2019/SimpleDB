package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;

    private int tableId;

    private OpIterator[] children;

    private boolean isFinished;

    private int count;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        tid = t;
        this.children = new OpIterator[1];
        children[0] = child;
        this.tableId = tableId;
        isFinished = false;
        count = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        children[0].open();
        while(children[0].hasNext()){
            Tuple cur = children[0].next();
            try{
                Database.getBufferPool().insertTuple(tid, tableId, cur);
                count ++;
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public void close() {
        // some code goes here
        children[0].close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        children[0].rewind();
        isFinished = false;
        count = 0;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(isFinished){
            return null;
        }
        Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0, new IntField(count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }

    public Tuple next() throws DbException, TransactionAbortedException{
        if(isFinished){
            return null;
        }
        Tuple tuple = super.next();
        isFinished = true;
        return tuple;
    }
}
