package src.Index;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class Node {

    private boolean internal;
    private Integer parent;
    private Integer self;
    private TreeMap<TreeMapObj, Integer> pageNums;
    private TreeMap<TreeMapObj, Integer> indexes;
    private Integer finalValue;
    private String keyType;

    public Node(boolean isInternal, Integer parent, String primaryKeyType, int self) {
        pageNums = new TreeMap<TreeMapObj, Integer>();
        indexes = new TreeMap<TreeMapObj, Integer>();
        finalValue = -1;
        this.internal = isInternal;
        this.parent = parent;
        this.keyType = primaryKeyType;
        this.self = self;
    }

    public Node(byte[] bytes, String primaryKeyType, int self) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        this.self = self;

        //get isInternal
        int internalNum = buffer.getInt();
        if (internalNum == 0) {
            this.internal = false;
        }
        else {
            this.internal = true;
        }

        //get parent
        this.parent = buffer.getInt();

        //get number of pageNums
        int numValues = buffer.getInt();

        this.keyType = primaryKeyType;

        //get pageNums
        this.pageNums = new TreeMap<TreeMapObj, Integer>();
        this.indexes = new TreeMap<TreeMapObj, Integer>();
        for (int i = 0; i < numValues; i++) {
            TreeMapObj key = new TreeMapObj(primaryKeyType, buffer);
            pageNums.put(key, buffer.getInt());
            if (!internal) {
                indexes.put(key, buffer.getInt());
            }
        }

        //get final value
        this.finalValue = buffer.getInt();
    }

    public TreeMap<TreeMapObj, Integer> getPageNums() {
        return pageNums;
    }

    public void setPageNums(TreeMap<TreeMapObj, Integer> pageNums) {
        this.pageNums = pageNums;
    }

    public TreeMap<TreeMapObj, Integer> getIndexes() {
        return indexes;
    }

    public void setIndexes(TreeMap<TreeMapObj, Integer> indexes) {
        this.indexes = indexes;
    }

    public Integer getFinalValue() {
        return finalValue;
    }

    public void setFinalValue(Integer finalValue) {
        this.finalValue = finalValue;
    }

    public boolean isInternal() {
        return internal;
    }

    public void setInternal(boolean isInternal) {
        this.internal = isInternal;
    }

    public Integer getParent() {
        return parent;
    }

    public void setParent(Integer parent) {
        this.parent = parent;
    }

    public int getSelf() {
        return self;
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[getNodeByteSize()]);

        //set isInternal
        if (this.internal) {
            buffer.putInt(1);
        }
        else {
            buffer.putInt(0);
        }

        //set parent
        buffer.putInt(this.parent);

        //set number of pageNums
        buffer.putInt(pageNums.size());

        //set pageNums and indexes
        for (Map.Entry<TreeMapObj, Integer> entry : pageNums.entrySet()) {
            //set key
            buffer.put(entry.getKey().toBytes());
            //set page num
            buffer.putInt(entry.getValue());
            if (!internal) {
                buffer.putInt(indexes.get(entry.getKey()));
            }
        }

        //set final value
        buffer.putInt(finalValue);

        return buffer.array();
    }

    public int getNodeByteSize() {
        int size = 0;

        //count isInternal
        size += Integer.BYTES;

        //count parent
        size += Integer.BYTES;

        //count number of pageNums
        size += Integer.BYTES;

        //count pageNums
        for (Map.Entry<TreeMapObj, Integer> entry : pageNums.entrySet()) {
            size += entry.getKey().getByteSize();
            size += Integer.BYTES;
            if (!internal) {
                size += Integer.BYTES;
            }
        }

        //count final value
        size += Integer.BYTES;


        //return size
        return size;
    }
}
