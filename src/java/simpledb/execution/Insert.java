package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;
    private OpIterator opIterator;
    private TransactionId transactionId;
    private DbFile dbFile;

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
        transactionId = t;
        opIterator = child;
        dbFile = Database.getCatalog().getDatabaseFile(tableId);
        tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        opIterator.open();
    }

    public void close() {
        super.close();
        opIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        opIterator.rewind();
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
        int count = 0;
        if (opIterator.hasNext()) {
            while (opIterator.hasNext()) {
                Tuple tuple = opIterator.next();
                try {
                    Database.getBufferPool().insertTuple(transactionId, dbFile.getId(), tuple);
                } catch (IOException e) {
                    throw new DbException("insert error");
                }
                count += 1;
            }
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(count));
            return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{opIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        opIterator = children[0];
    }
}
