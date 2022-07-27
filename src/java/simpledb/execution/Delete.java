package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

	private static final long serialVersionUID = 1L;
	private TupleDesc tupleDesc;
	private OpIterator opIterator;
	private TransactionId transactionId;

	/**
	 * Constructor specifying the transaction that this delete belongs to as
	 * well as the child to read from.
	 *
	 * @param t     The transaction this delete runs in
	 * @param child The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, OpIterator child) {
		transactionId = t;
		opIterator = child;
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
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 *
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		int count = 0;
		if (opIterator.hasNext()) {
			while (opIterator.hasNext()) {
				Tuple tuple = opIterator.next();
				try {
					Database.getBufferPool().deleteTuple(transactionId, tuple);
					count += 1;
				} catch (IOException e) {
					throw new DbException("delete error");
				}
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
