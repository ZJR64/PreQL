package src.Index;

import src.StorageManager.BufferManager;

import java.nio.Buffer;

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

    public int findRange(Object primaryKeyValueOne, Object primaryKeyValueTwo) {
        //TODO find range between two values
        return -1;
    }

    public int findNot(Object primaryKeyValue) {
        //TODO find indexes that do not comply
        return -1;
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
