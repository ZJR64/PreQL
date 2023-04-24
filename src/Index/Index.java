package src.Index;

import src.StorageManager.BufferManager;
import src.StorageManager.Record;

import java.nio.Buffer;
import java.util.ArrayList;

public class Index {

    private int root;
    private String tableName;
    private BufferManager bufferManager;

    public Index(BufferManager bufferManager, String name) {
        //TODO constructor for new index
        this.bufferManager = bufferManager; //TODO make new buffer manager????!!!!!????
        root = 0;
        this.tableName = name;
    }

    public Index(byte[] bytes) {
        //TODO constructor for index from bytes
    }

    public void addToIndex(Object primaryKeyValue) {
        //TODO add to the tree
    }

    public void removeFromIndex(Object primaryKeyValue) {
        //TODO remove from the tree
    }

    public int find(Object primaryKeyValue) {
        //TODO find the page of the given primaryKey
        return -1;
    }

    public void updateIndex(ArrayList<Record> records, int pageNumber) {
        //TODO update the index of whole page
    }

    public byte[] toBytes(){
        //TODO convert to byte array
        byte[] no = new byte[2];
        return no;
    }

    public int getIndexByteSize() {
        //TODO return the number of bytes needed for the index
        return 0;
    }
}
