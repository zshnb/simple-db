package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

	private static final long serialVersionUID = 1L;
	private OpIterator opIterator;
	private int aggregateFieldIndex;
	private int groupByFieldIndex;
	private Aggregator.Op op;
	private Aggregator aggregator;
	private TupleDesc tupleDesc;

	/**
	 * Constructor.
	 * <p>
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
	 * you with your implementation of readNext().
	 *
	 * @param child  The OpIterator that is feeding us tuples.
	 * @param afield The column over which we are computing an aggregate.
	 * @param gfield The column over which we are grouping the result, or -1 if
	 *               there is no grouping
	 * @param aop    The aggregation operator to use
	 */
	public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
		opIterator = child;
		aggregateFieldIndex = afield;
		groupByFieldIndex = gfield;
		op = aop;
		tupleDesc = opIterator.getTupleDesc();
		Type aggregateType = opIterator.getTupleDesc().getFieldType(aggregateFieldIndex);
		if (aggregateType.equals(Type.INT_TYPE)) {
			aggregator = new IntegerAggregator(groupByFieldIndex, Type.INT_TYPE, aggregateFieldIndex, op);
		} else {
			aggregator = new StringAggregator(groupByFieldIndex, Type.STRING_TYPE, aggregateFieldIndex, op);
		}
		try {
			child.open();
			while (child.hasNext()) {
				Tuple tuple = child.next();
				aggregator.mergeTupleIntoGroup(tuple);
			}
		} catch (TransactionAbortedException | DbException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @return If this aggregate is accompanied by a groupby, return the groupby
	 * field index in the <b>INPUT</b> tuples. If not, return
	 * {@link Aggregator#NO_GROUPING}
	 */
	public int groupField() {
		return groupByFieldIndex;
	}

	/**
	 * @return If this aggregate is accompanied by a group by, return the name
	 * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
	 * null;
	 */
	public String groupFieldName() {
		if (groupByFieldIndex == -1) {
			return null;
		}
		return tupleDesc.getFieldName(groupByFieldIndex);
	}

	/**
	 * @return the aggregate field
	 */
	public int aggregateField() {
		return aggregateFieldIndex;
	}

	/**
	 * @return return the name of the aggregate field in the <b>OUTPUT</b>
	 * tuples
	 */
	public String aggregateFieldName() {
		if (aggregateFieldIndex == -1) {
			return null;
		}
		return tupleDesc.getFieldName(aggregateFieldIndex);
	}

	/**
	 * @return return the aggregate operator
	 */
	public Aggregator.Op aggregateOp() {
		return op;
	}

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
	}

	public void open() throws NoSuchElementException, DbException,
		TransactionAbortedException {
		super.open();
		aggregator.iterator().open();
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate. If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		while (aggregator.iterator().hasNext()) {
			return aggregator.iterator().next();
		}
		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		aggregator.iterator().rewind();
	}

	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field,
	 * this will have one field - the aggregate column. If there is a group by
	 * field, the first field will be the group by field, and the second will be
	 * the aggregate value column.
	 * <p>
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
	 * given in the constructor, and child_td is the TupleDesc of the child
	 * iterator.
	 */
	public TupleDesc getTupleDesc() {
		return tupleDesc;
	}

	public void close() {
		super.close();
		aggregator.iterator().close();
	}

	@Override
	public OpIterator[] getChildren() {
		return new OpIterator[]{opIterator};
	}

	@Override
	public void setChildren(OpIterator[] children) {
		// some code goes here
	}

}
