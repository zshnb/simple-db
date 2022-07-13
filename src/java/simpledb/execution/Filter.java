package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * 实现where语句
 */
public class Filter extends Operator {
	private Predicate predicate;
	private OpIterator opIterator;

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor accepts a predicate to apply and a child operator to read
	 * tuples to filter from.
	 *
	 * @param p     The predicate to filter tuples with
	 * @param child The child operator
	 */
	public Filter(Predicate p, OpIterator child) {
		predicate = p;
		opIterator = child;
	}

	public Predicate getPredicate() {
		return predicate;
	}

	public TupleDesc getTupleDesc() {
		return opIterator.getTupleDesc();
	}

	public void open() throws DbException, NoSuchElementException,
		TransactionAbortedException {
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
	 * AbstractDbIterator.readNext implementation. Iterates over tuples from the
	 * child operator, applying the predicate to them and returning those that
	 * pass the predicate (i.e. for which the Predicate.filter() returns true.)
	 *
	 * @return The next tuple that passes the filter, or null if there are no
	 * more tuples
	 * @see Predicate#filter
	 */
	protected Tuple fetchNext() throws NoSuchElementException,
		TransactionAbortedException, DbException {
		// some code goes here
		while (opIterator.hasNext()) {
			Tuple tuple = opIterator.next();
			if (predicate.filter(tuple)) {
				return tuple;
			}
		}
		return null;
	}

	@Override
	public OpIterator[] getChildren() {
		return new OpIterator[] {opIterator};
	}

	@Override
	public void setChildren(OpIterator[] children) {
		opIterator = children[0];
	}

}
