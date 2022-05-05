package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private int id;
    private TupleDesc tupleDesc;
    private int totalPageNumber;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        id = f.getAbsoluteFile().hashCode();
        this.totalPageNumber = numPagesHelper();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
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
        return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageNo = pid.getPageNumber();
        byte[] readContent = new byte[BufferPool.getPageSize()];
        Page page = null;
        try{
            RandomAccessFile file = new RandomAccessFile(this.file, "r");
            file.seek(pageNo * BufferPool.getPageSize());
            file.read(readContent, 0, BufferPool.getPageSize() );
            page = new HeapPage((HeapPageId) pid, readContent);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        int pageSize = Database.getBufferPool().getPageSize();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek( pageSize * page.getId().getPageNumber());
        byte[] pageContent = page.getPageData();
        randomAccessFile.write(pageContent, 0, pageSize);
        totalPageNumber = numPagesHelper();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return totalPageNumber;
    }

    public int numPagesHelper(){
        int size = (int)this.file.length();
        return (int)Math.ceil((double) size/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        boolean finished = false;
        BufferPool bufferPool = Database.getBufferPool();
        List<Page> modifiedPages = new LinkedList();
        HeapPageId targetPageId  =  null;
        HeapPage targetPage = null;
        for(int i = 0 ; i < numPages(); i++){
            targetPageId = new HeapPageId(id, i);
            targetPage = (HeapPage) bufferPool.getPage(tid, targetPageId, Permissions.READ_WRITE);
            if(targetPage.getNumEmptySlots() > 0) {
                targetPage.insertTuple(t);
                finished = true;
                targetPage.markDirty(true, tid);
                modifiedPages.add(targetPage);
                break;
            }
        }
        if(!finished){
            targetPageId = new HeapPageId(id, numPages());
            targetPage = new HeapPage(targetPageId, new byte[BufferPool.getPageSize()]);
            byte[] pageContent = targetPage.getPageData();
            FileOutputStream output = new FileOutputStream(file, true);
            try {
                output.write(pageContent);
            }finally {
                output.close();
            }
            HeapPage page = (HeapPage) bufferPool.getPage(tid, targetPageId, Permissions.READ_WRITE);
            page.insertTuple(t);
            page.markDirty(true, tid);
            modifiedPages.add(page);
            totalPageNumber = numPagesHelper();
        }

//        try{
//            RandomAccessFile file = new RandomAccessFile(this.file, "w");
//            file.seek(targetPageId.getPageNumber() * BufferPool.getPageSize());
//            byte[] content = targetPage.getPageData();
//            file.write(content, 0, BufferPool.getPageSize() );
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> dirtyPages = new ArrayList();
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        dirtyPages.add(page);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIteraor(tid);
    }

    public class HeapFileIteraor implements DbFileIterator{

        public HeapFileIteraor(TransactionId tid){
            this.transactionId = tid;
        }

        public int curPageNo = -1;
        TransactionId transactionId;
        public HeapPage curPage;
        public Iterator<Tuple> tupleIt;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            curPageNo = 0;
            HeapPageId heapPageId = new HeapPageId(id, curPageNo);
            curPage = (HeapPage)Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_ONLY);
            tupleIt = curPage.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(curPageNo < 0){
                return false;
            }
            while(curPageNo < numPages()){
                if(tupleIt.hasNext()){
                    return true;
                }
                curPageNo++;
                if(curPageNo >= numPages()){
                    return false;
                }
                HeapPageId heapPageId = new HeapPageId(id, curPageNo);
                curPage = (HeapPage)Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_ONLY);
                tupleIt = curPage.iterator();
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(curPageNo < 0){
                throw new NoSuchElementException("iterator not open() yet");
            }
            return tupleIt.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            curPageNo = 0;
            HeapPageId heapPageId = new HeapPageId(id, curPageNo);
            curPage = (HeapPage)Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_ONLY);
            tupleIt = curPage.iterator();
        }

        @Override
        public void close() {
            curPageNo = -1;
            curPage = null;
            tupleIt = null;
        }
    }
}

