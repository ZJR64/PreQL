package src.Index;

import src.StorageManager.BufferManager;
import src.StorageManager.Record;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

public class Index {

    private int root;
    private String tableName;
    private BufferManager bufferManager;

    public Index(BufferManager bufferManager, String name) {
        this.bufferManager = bufferManager;
        root = 0;
        this.tableName = name;
    }

    public Index(BufferManager bufferManager, ByteBuffer buffer, String tableName) {
        this.bufferManager = bufferManager;
        this.root = buffer.getInt();
        this.tableName = tableName;
    }

    public void addToIndex(Object primaryKeyValue) {
        //TODO add to the tree
    }

    public void removeFromIndex(Object primaryKeyValue) {
        //TODO remove from the tree
    }

    public int[] find(Object primaryKeyValue) {
        //TODO get node
        Node currentNode = new Node(false);
        while (currentNode.isInternal()) {
            for (Map.Entry<Object, Integer> entry : currentNode.getPageNums().entrySet())
                System.out.println("Key = " + entry.getKey() +
                        ", Value = " + entry.getValue());
        }
        return null;
    }

    public void updateIndex(ArrayList<Record> records, int pageNumber) {
        //TODO update the index of whole page
    }

    public byte[] toBytes(){
        ByteBuffer buffer = ByteBuffer.wrap(new byte[getIndexByteSize()]);
        buffer.putInt(root);
        return buffer.array();
    }

    public int getIndexByteSize() {
        return Integer.BYTES;
    }
}
