package src.StorageManager;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The class for the buffer. The buffer hold pages to use and modify.
 * The buffer is created with a set size and uses the database's page size
 * in may of it's operations. This class primarily uses RandomAccessFile
 * to read and write to files. This helps when searching for specific
 * pages within the bytes of the file. The buffer manager handles all operations
 * for the buffer itself, from flushing pages to disks, to adding to files.
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class BufferManager {
    String storagePath;
    private int pageSize;
    private int bufferSize;
    private Map<String, byte[]> buffer;
    private ArrayList<String> recentlyUsed;

    /**
     * Constructor for the BufferManager object
     *
     * @param pageSize the size of each page of the database.
     * @param bufferSize how many pages the buffer can store at a time.
     * @param databaseLocation the path to the databse. The constructor adds \
     */
    public BufferManager(int pageSize, int bufferSize, String databaseLocation) {
        this.pageSize = pageSize;
        this.bufferSize = bufferSize;
        this.buffer = new HashMap<String, byte[]>();
        buffer.clear();
        recentlyUsed = new ArrayList<String>();
        this.storagePath = databaseLocation + File.separator;
    }

    /**
     * Getter method for the page size
     *
     * @return the page size.
     */
    public int getPageSize(){return pageSize;}

    /**
     * Gets the desired page from the provided file location.
     * If the page is not already in the buffer it is retrieved from
     * storage.
     * It then returns the bytes from the page.
     *
     * @param fileName the name of the file.
     * @param pageNumber the page of the file to look for.
     * @return the bytes making up the page.
     */
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
        //modify recently used for LRU
        else {
            recentlyUsed.add(recentlyUsed.remove(recentlyUsed.indexOf(pageKey)));
        }

        return pageData;
    }

    /**
     * Writes the desired page to the provided file location.
     * The page must already be in the buffer to write to it.
     *
     * @param fileName the name of the file.
     * @param pageNumber the page of the file to look for.
     * @param pageData the data for the page.
     */
    public void writePage(String fileName, int pageNumber, byte[] pageData) {
        String pageKey = getPageKey(fileName, pageNumber);

        if (buffer.containsKey(pageKey)) {
            // Page is in the buffer, so overwrite the data
            buffer.put(pageKey, pageData);
        }
        else {
            System.err.println("Page " + pageKey + " is not in the buffer");
        }

    }

    /**
     * Adds a new page to the end of the given file.
     * Adds the created page to the buffer manager.
     *
     * @param fileName the name of the file.
     * @param openPages the list of open pages from schema.
     * @return the page number of the new page.
     */
    public int addPage(String fileName, ArrayList<Integer> openPages) {
        //if the try method fails, returns -1 to indicate failure
        int pageNumber = -1;

        //check if file exists, create it if it doesn't
        File tableFile = new File(storagePath + fileName);
        if (!tableFile.exists()) {
            try {
                tableFile.createNewFile();

                //create ByteBuffer and file
                ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.SIZE/Byte.SIZE]);
                RandomAccessFile file = new RandomAccessFile(storagePath + fileName, "rw");

                //write 0 page number to buffer
                buffer.putInt(0);

                //store
                file.seek(0);
                file.write(buffer.array());

                file.close();
            } catch (Exception e) {
                System.err.println("Could not create file");
                System.err.println(e);
            }
        }

        try {
            RandomAccessFile file = new RandomAccessFile(storagePath + fileName, "rw");
            //find end of file
            long fileLength = file.length();

            file.seek(fileLength);
            //get page number
            if (openPages.isEmpty()) {
                //get pageCount
                byte[] numPagesArray = new byte[Integer.SIZE/Byte.SIZE];
                file.seek(0);
                file.read(numPagesArray);
                ByteBuffer buffer = ByteBuffer.wrap(numPagesArray);

                //get number then increment and return to buffer
                pageNumber = buffer.getInt();
                buffer.putInt(0,pageNumber + 1);

                //store
                file.seek(0);
                file.write(buffer.array());
            }
            else {
                pageNumber = openPages.indexOf(0);
            }

            //write some nonsense bytes to add to the file
            byte[] padding = new byte[pageSize];
            file.write(padding);

            //add to buffer
            getPage(fileName, pageNumber);

            //close
            file.close();

        } catch (IOException e) {
            System.err.println("Could not add page to file " + fileName);
            System.err.println(e);
        }
        return pageNumber;
    }

    /**
     * Method that deletes the file and purges pages from buffer.
     *
     * @param fileName the name of the file to be deleted.
     */
    public void purge(String fileName) {
        //find any pages that need to be removed
        for (String key : buffer.keySet()) {
            //if page from file, remove from buffer
            if (key.startsWith(fileName)) {
                buffer.remove(key);
                recentlyUsed.remove(key);
            }
        }
        //delete file
        File tableFile = new File(storagePath + fileName);
        tableFile.delete();
    }

    /**
     * Removes page from buffer.
     * WARNING: DOES NOT SAVE THE PAGE
     *
     * @param fileName the filename the page belongs to.
     * @param pageNum the number of the page.
     */
    public void removePage(String fileName, int pageNum) {
        //create key
        String key = getPageKey(fileName, pageNum);

        //remove from buffer
        buffer.remove(key);
        recentlyUsed.remove(key);
    }

    /**
     * Method that safely stores the Catalog when the database is shut down.
     */
    public void shutDown(){
        // Flush all pages to disk
        for (String pageKey : buffer.keySet()) {
            try {
                flush(pageKey);
            } catch(Exception e) {
                System.err.println("Could not flush " + pageKey);
                System.err.println(e);
            }
        }
        //cleanse buffer
        buffer.clear();
        recentlyUsed.clear();
    }

    /**
     * Flushes the page to storage. Uses RanomAccessFile to
     * write to specific bytes of the file.
     *
     * @param pageKey the key for the page to be written to disk.
     * @throws IOException
     */
    private void flush(String pageKey) throws IOException{
        //brake down dict key
        String[] parts = pageKey.split(":");
        String fileName = parts[0];
        int pageNumber = Integer.parseInt(parts[1]);
        byte[] pageData = buffer.get(pageKey);

        //open file to write to
        RandomAccessFile file = new RandomAccessFile(storagePath + fileName, "rw");

        // Calculate the offset for the page within the file
        long offset = (long) pageNumber * pageSize;


        // Write the page data to the file at the calculated offset
        file.seek((Integer.SIZE / 8) + offset);
        file.write(pageData);

        file.close();
    }

    /**
     * Adds a page to the buffer, and if the buffer is full
     * it removes that least recently used page and flushes it.
     *
     * @param pageKey the key for the page in the dictionary.
     * @param pageData the data to be written to the new page.
     * @throws IOException
     */
    private void addPageToBuffer(String pageKey, byte[] pageData) throws IOException{
        if (buffer.size() == bufferSize) {
            // if buffer is full remove least recently used page
            String leastRecentlyUsedPageKey = recentlyUsed.remove(0);
            flush(leastRecentlyUsedPageKey);
            buffer.remove(leastRecentlyUsedPageKey);
        }
        recentlyUsed.add(pageKey);
        buffer.put(pageKey, pageData);
    }

    private String getPageKey(String fileName, int pageNumber) {
        return fileName + ":" + pageNumber;
    }

    /**
     * Method used to read the page from the file.
     *
     * @param fileName the name of the file.
     * @param pageNumber the number of the page to be read in.
     * @return the page number of the new page.
     * @throws IOException
     */
    private byte[] readPageFromFile(String fileName, int pageNumber) throws IOException {
        RandomAccessFile file = new RandomAccessFile(storagePath + fileName, "r");
        //make array of pageSize length
        byte[] pageData = new byte[pageSize];
        //calculate offset
        long offset =  (long) pageNumber * pageSize;

        //find the data and read it in
        file.seek((Integer.SIZE / 8) + offset);
        file.readFully(pageData);
        file.close();

        //return the data of the page
        return pageData;
    }
}
