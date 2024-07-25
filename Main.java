//functions are pretty much self explanatory as the name can pretty much says what the function doies
//References//
//ChatGBT//
//Stack overflow//
//piazza//
import java.sql.SQLException;
import java.util.Scanner;
import java.util.Set;
import java.lang.String;
public class Main {
    static Backend backend;
    static Customer user;
    static Scanner reader = new Scanner(System.in);
    static boolean marketOpen = true;
    public static void main(String[] args) throws SQLException {
        backend = new Backend();
        greeting();
        cus_interface();
    }
    private static void greeting(){
        System.out.println("Welcome to starRus");
        loginSignup();
    }
    static String requestChoice(String prompt, Set<String> options) {
        while (true) {
            System.out.print(prompt);
            if (reader.hasNextLine()) {
                String inputLine = reader.nextLine().toLowerCase();
                if (options.contains(inputLine))
                    return inputLine;
                else
                    System.out.println("Invalid input!");
            }
        }
    }
    private static void cus_interface(){
        while (true) {
            int group = user.username.equals("admin") ? 0 : 1;
            if (group == 1) { // customer
                Set<String> array = Set.of("deposit", "withdraw", "buy", "sell", "balance", "history", "stock price", "movie", "top movies", "reviews", "cancel", "exit");
                String choice = requestChoice("Deposit, Withdraw, Buy, Sell, Balance, History, Stock Price, Movie, Top Movies, Reviews, Cancel, or Exit: ", array);
                if (choice.equals("deposit")) {
                    deposit();
                } 
                else if (choice.equals("withdraw")) {
                    withdraw();
                } 
                else if (choice.equals("buy")) {
                    buy();
                } 
                else if (choice.equals("sell")) {
                    sell();
                } 
                else if (choice.equals("balance")) {
                    backend.getBalance(user);
                } 
                else if (choice.equals("history")) {
                    backend.getTransactionHistory(user);
                } 
                else if (choice.equals("stock price")) {
                    stockInfo();
                } 
                else if (choice.equals("movie")) {
                    movieInfo();
                } 
                else if (choice.equals("top movies")) {
                    topMovies();
                } 
                else if (choice.equals("reviews")) {
                    movieReviews();
                }
                else if (choice.equals("cancel")){
                    backend.cancelTrans(user,backend.getDate());
                }
                else if (choice.equals("exit")) {
                    loginSignup();
                }
            }
            else{
                Set<String> array = Set.of("add interest", "monthly statement", "list active", "dter", "customer report", "delete transaction", "set date", "toggle", "set price", "set interest", "exit");
                String choice = requestChoice("Add interest, Monthly statement, list active, DTER, customer report, delete transaction, set date, toggle, set price, set interest, or Exit: ", array);
                if (choice.equals("add interest")) {
                    backend.manualAccureInterest();
                } 
                else if (choice.equals("monthly statement")) {
                    monthlyStatement();
                } 
                else if (choice.equals("list active")) {
                    backend.generateActiveUserList();
                } 
                else if (choice.equals("dter")) {
                    backend.generateDTER();
                } 
                else if (choice.equals("customer report")) {
                    customerReport();
                } 
                else if (choice.equals("delete transaction")) {
                    deleteTransactions();
                } 
                else if (choice.equals("set date")) {
                    setDate();
                } 
                else if (choice.equals("toggle")) {
                    toggleMarket();
                } 
                else if (choice.equals("set price")) {
                    setPrice();
                } 
                else if (choice.equals("set interest")) {
                    Backend.interest = requestDouble("Enter new interest ", 0.01);
                    System.out.println("New interest set " + Backend.interest);
                } 
                else if (choice.equals("exit")) {
                    loginSignup();
                }
            }
        }
    }
    private static void deleteTransactions() {
        backend.deleteTransaction(padZero(Integer.toString(requestInt("Enter the date you want to delete Transactions \n ex. 01011970 will delete Transactions from 01/01/1970 to 01/31/1970 \n", 10011970)),8));
    }
    static String padZero (String str, int amount){
        while (str.length() < amount){
            str = "0" + str;
        }
        return str;
    }
    private static void toggleMarket(){
        Set<String> array = Set.of("True", "False");
        String choice = requestChoice("Enter value: True/False ", array);
        if (choice.equals("true")) {
            marketOpen = true;
            System.out.println("Market is opened.");
        } 
        else if (choice.equals("false")) {
            marketOpen = false;
            System.out.println("Market is closed.");
        }
    }
    private static void setPrice(){
        backend.setStockPrice(requestString("Enter name of the stock: ", false), requestDouble("Enter new price: ", 0));
    }
    private static void setDate(){
        backend.setDate(padZero(Integer.toString(requestInt("Enter date (ex. 0101970): ", 10011970)),8));
    }
    private static void customerReport(){
        backend.generateCustomerReport(padZero(Integer.toString(requestInt("Enter customer Tax ID: ", 000000000)),9));
    }
    private static void monthlyStatement(){
        backend.generateMonthlyStatement(backend.queryCustomer(padZero(Integer.toString(requestInt("Enter customer Tax ID: ", 000000000)),9)));
    }
    private static void stockInfo(){
        backend.getStockInfo(requestString("Enter the name of the stock: ", false));
    }
    private static void movieInfo(){
        Set<String> array = Set.of("list all", "check info", "exit");
        String choice = requestChoice("List all, Check Info or Exit: ", array);
        if (choice.equals("list all")) {
            backend.getAllMovies();
            movieInfo();
        } else if (choice.equals("check info")) {
            checkOneInfo();
            movieInfo();
        } else if (choice.equals("exit")) {
            return;
        }
    }
    private static void movieReviews(){
        backend.getMovieReviews(requestString("Enter the name of the movie: ", false));
    }
    private static void topMovies(){
        backend.topMovies(Integer.toString(requestInt("Enter the start year: ", 10011970)), Integer.toString(requestInt("Enter the end year: ", 12311970)));
    }
    static int requestInt(String prompt, int defaultValue) {
        while (true) {
            System.out.print(prompt);
            if (reader.hasNextLine()) {
                try {
                    return Integer.parseInt(reader.nextLine());
                } catch (NumberFormatException e) {
                    System.out.println("Please input a number. Try again." + e);
                }
            } else {
                return defaultValue;
            }
        }
    }
    private static void checkOneInfo(){
        backend.getOneMovie(requestString("Enter the name of the movie: ", false));
    }
    private static void buy(){
        if (!marketOpen){
            System.out.println("Market currently closed, no transaction!");
            return;
        }
        backend.buyStock(user, requestString("Enter the name of the stock: ", false), requestDouble("Enter the amount you want to buy: ", 0));
    }
    private static void deposit(){
        backend.deposit(user, requestDouble("Enter the amount you want to deposit: ", 0), "deposit");
    }
    private static void sell(){
        if (!marketOpen){
            System.out.println("Market currently closed, no transaction!");
            return;
        }
        backend.sellStock(user, requestString("Enter the name of the stock: ", false), requestDouble("Enter the original price: ", 0), requestDouble("Enter the amount you want to sell: ", 0));
    }
    private static void withdraw(){
        backend.withdraw(user, requestDouble("Enter the amount you want to withdraw: ", 0), "withdraw");
    }
    static double requestDouble(String prompt, double defaultValue) {
        while (true) {
            System.out.print(prompt);
            if (reader.hasNextLine()) {
                try {
                    return Double.parseDouble(reader.nextLine());
                } catch (NumberFormatException e) {
                    System.out.println("Please input a number. Try again.");
                }
            } else {
                return defaultValue;
            }
        }
    }
    private static void loginSignup() {
        Set<String> array = Set.of("sign up", "log in", "exit");
        String choice = requestChoice("Sign up, Log in, or Exit: ", array);
        if (choice.equals("sign up")) {
            signup(); 
            loginSignup();
            System.out.println("Sign up");
        } else if (choice.equals("log in")) {
            login();
            System.out.println("Log in");
        } else if (choice.equals("exit")) {
            System.out.println("Exit ");
            farewell(); 
            backend.exit(); 
            System.exit(0);
        }
    }
    private static void login() {
        user = new Customer(requestString("Username: ", false), requestString("Password: ", false));
        if (backend.login(user)){
            System.out.println("Welcome, " + user.cname + "! (tax id: " + user.tax_id + ", email: " + user.email_add + ")");
            System.out.println("Today is: " + backend.getDate());
        }
        else {
            login();
        }
    }
    static String requestString(String prompt, boolean optional) {
        while (true) {
            System.out.print(prompt);
            if (reader.hasNextLine()) {
                String line = reader.nextLine();
                if (!optional && line.equals("")) {
                } else {
                    return line;
                }
            }
        }
    }
    private static void signup() {
        backend.signup(requestString("Tax ID: ", false), requestString("Name: ", false), requestString("State: ", false), requestString("Phone Number: ", false), requestString("Email: ", false), requestString("Username: ", false), requestString("Password: ", false));
    }
    private static void farewell() {
        System.out.println("Good bye");
    }
}
