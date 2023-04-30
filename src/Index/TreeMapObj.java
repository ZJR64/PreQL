package src.Index;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This is a special object for the TreeMaps to be able to properly sort, and
 * still work with primitive types (String, integer, double).
 */
public class TreeMapObj implements Comparable<TreeMapObj> {
    private String type;
    private Object primaryKeyValue;

    /**
     * A TreeMapObj consists of a type variable (the type of the primaryKeyValue being passed in)
     * and an object that is the primaryKeyValue.
     * @param type the type of the primaryKeyValue
     * @param primaryKeyValue the actual object of the primaryKeyValue
     */
    public TreeMapObj(String type, Object primaryKeyValue){
        this.type = type;
        this.primaryKeyValue = primaryKeyValue;
    }

    public TreeMapObj(String type, ByteBuffer buffer) {
        int size = buffer.getInt();
        this.type = type;

        if (type.contains("char")) {
            //string
            byte[] stringArray = new byte[size];
            buffer.get(stringArray);
            this.primaryKeyValue = new String(stringArray);
        }
        else if (type.equalsIgnoreCase("integer")) {
            //integer
            this.primaryKeyValue = buffer.getInt();
        }
        else if (type.equalsIgnoreCase("boolean")) {
            //boolean
            int key = buffer.getInt();
            if (key == 1) {
                this.primaryKeyValue = true;
            }
            else {
                this.primaryKeyValue = false;
            }
        }
        else {
            //double
            this.primaryKeyValue = buffer.getDouble();
        }
    }


    public String getType(){
        return this.type;
    }

    public Object getPrimaryKeyValue(){
        return this.primaryKeyValue;
    }


    public int compareTo(TreeMapObj otherTree){
        try {
            if (this.type.equalsIgnoreCase(otherTree.type)) {
                if (type.contains("char")) {
                    String thisPK = (String) primaryKeyValue;
                    String otherPK = (String) otherTree.primaryKeyValue;
                    return thisPK.compareTo(otherPK);
                } else if (type.equalsIgnoreCase("Integer")) {
                    int thisPK = (Integer) primaryKeyValue;
                    int otherPK = (Integer) otherTree.primaryKeyValue;
                    return Integer.compare(thisPK, otherPK);
                } else if (type.equalsIgnoreCase("Double")) {
                    Double thisPK = (Double) primaryKeyValue;
                    Double otherPK = (Double) otherTree.primaryKeyValue;
                    return thisPK.compareTo(otherPK);
                } else {
                    System.out.println("ERROR COMPARING TREE OBJECTS! (non-valid type)");
                    return -10000;
                }
            } else {
                System.out.println("ERROR COMPARING TREE OJECTS! (incompatible types)");
                return -10000;
            }
        }
        catch(Exception e){
            e.printStackTrace();
            return -10000;
        }
    }


    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[getByteSize()]);
        if (this.type.contains("char")) {
            //string
            String key = (String) this.primaryKeyValue;
            byte[] stringBytes = key.getBytes();
            buffer.putInt(stringBytes.length);
            buffer.put(stringBytes);
        }
        else if (this.type.equalsIgnoreCase("integer")) {
            //integer
            int key = (int) this.primaryKeyValue;
            buffer.putInt(Integer.BYTES);
            buffer.putInt(key);
        }
        else if (this.type.equalsIgnoreCase("boolean")) {
            //boolean
            buffer.putInt(Integer.BYTES);
            if ((boolean) this.primaryKeyValue) {
                buffer.putInt(1);
            }
            else {
                buffer.putInt(0);
            }
        }
        else {
            //double
            double key = (double) this.primaryKeyValue;
            buffer.putInt(Double.BYTES);
            buffer.putDouble(key);
        }
        return buffer.array();
    }


    public int getByteSize() {
        int size = 0;

        //count object length
        size += Integer.BYTES;

        //count object
        if (type.contains("char")) {
            //string
            String counting = (String) this.primaryKeyValue;
            size += counting.getBytes().length;
        }
        else if (type.equalsIgnoreCase("integer")) {
            //integer
            size += Integer.BYTES;
        }
        else if (type.equalsIgnoreCase("boolean")) {
            //boolean
            size += Integer.BYTES;
        }
        else {
            //double
            size += Double.BYTES;
        }

        return size;
    }

}
