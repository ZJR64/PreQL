package src.Index;

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


    public String getType(){
        return this.type;
    }

    public Object getPrimaryKeyValue(){
        return this.primaryKeyValue;
    }


    public int compareTo(TreeMapObj otherTree){
        try {
            if (this.type.equalsIgnoreCase(otherTree.type)) {
                if (type.equalsIgnoreCase("String")) {
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
                    System.out.println("ERROR COMPARING TREE OJECTS! (non-valid type)");
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

}
