package src.Catalog;

public class Attribute {

    private String type;
    private String name;

    public Attribute (String type, String name) {
        this.name = name;
        this.type = type;
    }

    /**
     * toString method for the schema that is used when the schema needs to be displayed.
     *
     * @return the schema in string form.
     */
    @Override
    public String toString() {
        String output = type + " " + name;
        return output;
    }

}
