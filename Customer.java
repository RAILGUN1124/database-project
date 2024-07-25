

public class Customer {
    String tax_id;
    String cname;
    String state;
    String phone_num;
    String email_add;
    String username;
    String password;

    public Customer(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public Customer(String tax_id, String cname, String state, String phone_num, String email_add, String username, String password) {
        this.tax_id = tax_id;
        this.cname = cname;
        this.state = state;
        this.phone_num = phone_num;
        this.email_add = email_add;
        this.username = username;
        this.password = password;
    }

    @Override
    public String toString() {
        return "[" + this.tax_id + "] " + this.username;
    }
}
