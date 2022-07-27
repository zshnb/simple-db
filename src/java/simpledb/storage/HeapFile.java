package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
	private File file;
	private TupleDesc tupleDesc;
	// 页数
	private int pageCount;
	private int id;

	/**
	 * Constructs a heap file backed by the specified file.
	 *
	 * @param f the file that stores the on-disk backing store for this heap
	 *          file.
	 */
	public HeapFile(File f, TupleDesc td) {
		pageCount = 1;
		this.file = f;
		this.tupleDesc = td;
		this.id = new Random().nextInt();
	}

	public void setPageCount(int pageCount) {
		this.pageCount = pageCount;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return file;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere to ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return id;
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 *
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return tupleDesc;
	}

	/**
	 * 从file中读取指定page，需要计算offset,使用RandomAccessFile读取，offset=pageNumber * pageSize
	 */
	public Page readPage(PageId pid) {
		int offset = pid.getPageNumber() * BufferPool.getPageSize();
		try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
			randomAccessFile.seek(offset);
			byte[] bytes = new byte[BufferPool.getPageSize()];
			randomAccessFile.read(bytes);
			return new HeapPage((HeapPageId) pid, bytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		PageId pageId = page.getId();
		int offset = pageId.getPageNumber() * BufferPool.getPageSize();
		RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
		randomAccessFile.seek(offset);
		randomAccessFile.write(page.getPageData());
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return pageCount;
	}

	// see DbFile.java for javadocs
	/**
	 * 找到有空余位置的page（如果没有创建一页），插入tuple
	 * */
	public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
		HeapPageId heapPageId = new HeapPageId(id, pageCount - 1);
		Page page = Database.getBufferPool().getPage(tid, heapPageId, null);
		if (page == null || ((HeapPage) page).getNumEmptySlots() == 0) {
			page = new HeapPage(new HeapPageId(id, pageCount), new byte[BufferPool.getPageSize()]);
			pageCount += 1;
		}
		((HeapPage) page).insertTuple(t);
		writePage(page);
		return List.of(page);
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
		TransactionAbortedException {
		ArrayList<Page> pages = new ArrayList<>();
		int pageNumber = 0;
		while (true) {
			HeapPageId heapPageId = new HeapPageId(id, pageNumber);
			Page page = Database.getBufferPool().getPage(tid, heapPageId, null);
			if (page == null) {
				break;
			}
			if (page instanceof HeapPage) {
				HeapPage heapPage = (HeapPage) page;
				try {
					heapPage.deleteTuple(t);
					heapPage.markDirty(true, tid);
					pages.add(heapPage);
					if (heapPage.getNumEmptySlots() == heapPage.numSlots) {
						pageCount -= 1;
					}
					writePage(heapPage);
					break;
				} catch (DbException e) {
					pageNumber += 1;
				} catch (IOException e) {
					throw new DbException("write page error");
				}
			}
		}
		return pages;
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		return new HeapFileIterator(tid, id, pageCount);
	}
}

class HeapFileIterator extends AbstractDbFileIterator {
	private Iterator<Tuple> iterator;
	private HeapPage page;
	private int pageNumber;
	private int pageCount;
	private TransactionId tid;
	private int tableId;

	public HeapFileIterator(TransactionId tid, int tableId, int pageCount) {
		this.tid = tid;
		this.tableId = tableId;
		this.pageCount = pageCount;
		pageNumber = 0;
	}

	@Override
	protected Tuple readNext() throws DbException, TransactionAbortedException {
		if (iterator == null) {
			return null;
		}
		while (pageNumber < pageCount && !iterator.hasNext()) {
			pageNumber += 1;
			page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pageNumber), null);
			iterator = page.iterator();
		}
		if (iterator.hasNext()) {
			return iterator.next();
		}
		return null;
	}

	@Override
	public void open() throws DbException, TransactionAbortedException {
		page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pageNumber), null);
		iterator = page.iterator();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		iterator = page.iterator();
	}

	@Override
	public void close() {
		super.close();
		iterator = null;
	}
}
