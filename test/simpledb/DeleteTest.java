package simpledb;

import junit.framework.JUnit4TestAdapter;
import org.junit.Before;
import org.junit.Test;
import simpledb.common.Utility;
import simpledb.execution.Delete;
import simpledb.execution.Insert;
import simpledb.execution.OpIterator;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * We reserve more heavy-duty insertion testing for HeapFile and HeapPage.
 * This suite is superficial.
 */
public class DeleteTest extends TestUtil.CreateHeapFile {

	private OpIterator scan1;
	private TransactionId tid;

	/**
	 * Initialize each unit test
	 */
	@Before
	public void setUp() throws Exception {
		super.setUp();
		tid = new TransactionId();
	}

	/**
	 * Unit test for Insert.getTupleDesc()
	 */
	@Test
	public void getTupleDesc() throws Exception {
		Insert op = new Insert(tid, scan1, empty.getId());
		TupleDesc expected = Utility.getTupleDesc(1);
		TupleDesc actual = op.getTupleDesc();
		assertEquals(expected, actual);
	}

	/**
	 * Unit test for Insert.getNext(), inserting elements into an empty file
	 */
	@Test
	public void getNext() throws Exception {
		List<Tuple> tuples = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			Tuple tup = new Tuple(Utility.getTupleDesc(2));
			tup.setRecordId(new RecordId(new HeapPageId(1, 0), i));
			for (int j = 0; j < 2; ++j) {
				tup.setField(j, new IntField(i + 1));
			}
			empty.insertTuple(tid, tup);
			tuples.add(tup);
		}
		scan1 = new TupleIterator(Utility.getTupleDesc(2), tuples);
		scan1.open();

		SeqScan seqScan = new SeqScan(tid, empty.getId());
		seqScan.open();
		TestUtil.matchAllTuples(scan1, seqScan);

		Delete op = new Delete(tid, scan1);
		op.open();
		assertTrue(TestUtil.compareTuples(Utility.getHeapTuple(3, 1), op.next()));
		// we should fit on one page
		assertEquals(0, empty.numPages());

		seqScan = new SeqScan(tid, empty.getId());
		seqScan.open();
		boolean hasResult = false;
		while (seqScan.hasNext()) {
			hasResult = true;
			seqScan.next();
		}
		assertFalse(hasResult);
	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(DeleteTest.class);
	}
}

