package src.Commands;

public class Update extends Command{

    // table name
    String name;
    // update the columnToSet to the valueToSet
    // column to update
    String columnToSet;
    // value to set
    String valueToSet;
    // where clause
    // will be null if no where
    WhereClause where;

    /**
     * Constructor for the Command object. Used to store  and manipulate the input from
     * the classes that extend this one.
     *
     * @param input the entire input from the user.
     */
    public Update(String input) {
        super(input);
        try{
            // split on keyword set
            String[] splitInput = input.split("(?i)set");
            this.name = splitInput[0].replace("update", "").strip();
            // if there's no where clause
            if(!splitInput[1].toLowerCase().contains("where")){
                String[] splitSet = splitInput[1].split("=");
                this.columnToSet = splitSet[0].strip();
                this.valueToSet = splitSet[1].replace(";", "").strip();
                this.where = null;
            }
            // otherewise there's a where
            else{
                String[] whereSplit = splitInput[1].split("(?i)where");
                // first half is set values
                String[] splitSet = whereSplit[0].split("=");
                this.columnToSet = splitSet[0].strip();
                this.valueToSet = splitSet[1].replace(";", "").strip();
                // second half is where clause
                this.where = new WhereClause(whereSplit[1].replace(";", "").strip());
            }
            this.success = true;
        }
        catch(Exception e){
            System.out.println(input + " could not be parsed");
            this.success = false;
        }
    }

    @Override
    public void parse() {

    }

    @Override
    public String execute() {
        return null;
    }
}
