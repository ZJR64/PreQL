package src.StorageManager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class BufferManager {
    private int pageSize;
    private int bufferSize;
    private HashMap<String, byte[]> buffer;

    public BufferManager(int pageSize, int bufferCapacity) {
        this.pageSize = pageSize;
        this.bufferSize = bufferCapacity;
        this.buffer = new HashMap<String, byte[]>();
    }

    public byte[] getPage(String fileName, int pageNumber) {
        String pageKey = getPageKey(fileName, pageNumber);
        byte[] pageData = buffer.get(pageKey);

        if (pageData == null) {
            // Page not in buffer, read from file and add to buffer
            try {
                pageData = readPageFromFile(fileName, pageNumber);
                addPageToBuffer(pageKey, pageData);
            } catch (Exception e) {
                System.err.println("Could not read from file");
                System.err.println(e);
            }
        }

        return pageData;
    }

    private void writePage(String fileName, int pageNumber, byte[] pageData) throws IOException {
        String pageKey = getPageKey(fileName, pageNumber);

        if (buffer.get(pageKey) != null) {
            // Page is in the buffer, so overwrite the data
            buffer.put(pageKey, pageData);
        }
        else {
            System.err.println("Page " + pageKey + " is not in the buffer");
        }

    }

    public void shutDown(){
        // Flush all dirty pages to disk
        for (String pageKey : buffer.keySet()) {
            try {
                flush(pageKey);
            } catch(Exception e) {
                System.err.println("Could not flush " + pageKey);
                System.err.println(e);
            }
        }
    }

    private void flush(String pageKey) throws IOException{
        //brake dwn dict key
        String[] parts = pageKey.split(":");
        String fileName = parts[0];
        int pageNumber = Integer.parseInt(parts[1]);
        byte[] pageData = buffer.get(pageKey);

        //open file to write to
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");

        // Calculate the offset for the page within the file
        long offset = (long) pageNumber * pageSize;


        // Write the page data to the file at the calculated offset
        file.seek(offset);
        file.write(pageData);

        file.close();
    }

    private void addPageToBuffer(String pageKey, byte[] pageData) throws IOException{
        if (buffer.size() == bufferSize) {
            // if buffer is full remove least recently used page
            String leastRecentlyUsedPageKey = buffer.keySet().iterator().next();
            flush(leastRecentlyUsedPageKey);
            buffer.remove(leastRecentlyUsedPageKey);
        }
        buffer.put(pageKey, pageData);
    }

    private String getPageKey(String fileName, int pageNumber) {
        return fileName + ":" + pageNumber;
    }

    private byte[] readPageFromFile(String fileName, int pageNumber) throws IOException {
        RandomAccessFile file = new RandomAccessFile(fileName, "r");
        //make array of pageSize length
        byte[] pageData = new byte[pageSize];
        //calculate offset
        long offset =  (long) pageNumber * pageSize;

        //find the data and read it in
        file.seek(offset);
        file.readFully(pageData);
        file.close();

        //return the data of the page
        return pageData;
    }
}
