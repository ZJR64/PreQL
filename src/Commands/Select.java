package src.Commands;

public class Select extends Command {
    private String name;

    public Select(String input){
        super(input);
    }

    @Override
    public void parse(){
        // split on the keyword from
        // should return ["select *", <name>]
        String[] split = input.split("(?i)from");
        // remove semicolon and white space
        this.name = split[1].replace(";", "").strip();
    }

    @Override
    public String execute() {
        return "ERROR";
    }


}
