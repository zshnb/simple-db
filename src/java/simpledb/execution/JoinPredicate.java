package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * 比较某两行数据的指定列是否相等，等价与join on xxx = yyy
 */
public class JoinPredicate implements Serializable {

	private static final long serialVersionUID = 1L;
	private int firstFieldIndex;
	private int secondFieldIndex;
	private Predicate.Op op;

	/**
	 * Constructor -- create a new predicate over two fields of two tuples.
	 *
	 * @param field1 The field index into the first tuple in the predicate
	 * @param field2 The field index into the second tuple in the predicate
	 * @param op     The operation to apply (as defined in Predicate.Op); either
	 *               Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
	 *               Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
	 *               Predicate.Op.LESS_THAN_OR_EQ
	 * @see Predicate
	 */
	public JoinPredicate(int field1, Predicate.Op op, int field2) {
		firstFieldIndex = field1;
		secondFieldIndex = field2;
		this.op = op;
	}

	/**
	 * Apply the predicate to the two specified tuples. The comparison can be
	 * made through Field's compare method.
	 *
	 * @return true if the tuples satisfy the predicate.
	 */
	public boolean filter(Tuple t1, Tuple t2) {
		Field firstField = t1.getField(firstFieldIndex);
		Field secondField = t2.getField(secondFieldIndex);
		return firstField.compare(op, secondField);
	}

	public int getField1() {
		return firstFieldIndex;
	}

	public int getField2() {
		return secondFieldIndex;
	}

	public Predicate.Op getOperator() {
		return op;
	}
}
