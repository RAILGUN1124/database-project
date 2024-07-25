//functions are pretty much self explanatory as the name can pretty much says what the function doies
//Responsible for handling requests from frontend
//References//
//ChatGBT//
//Stack overflow//
//piazza//
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
public class Backend{
    private Server server;
    public static double interest = 0.01;
    public Backend() throws SQLException {
        server = new Server();
    }
    public Backend(Server server) {
        this.server = server;
    }
    public void exit() {
        server.exit();
    }
    static int hasPassed1000 (Server server, String tid, String date){
        return getBuyAmount(server, tid, date) - getSellAmount(server, tid, date);
    }
    boolean generateDTER (){
        System.out.println("Beginning of DTER List");
        System.out.println("|TaxID|Name|Earning|State|");
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Customer");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String tid = resultSet.getString(1);
                if(tid.equals("000001000"))
                    continue;
                double total = getTotalEarnings(server, tid, getMARKETACCOUNTNumber(server,tid), getDate());
                if (total > 10000.0){
                    System.out.println("|" + tid + "|" + resultSet.getString(2) + "|" + Sformat(total) + "|" + resultSet.getString(3) + "|");
                }
            }
        } catch (SQLException ignored) {
            System.out.println("Failed login w/ SQLException" + ignored);
            return false;
        }
        System.out.println("End of DTER List");
        return true;
    }
    
    boolean deleteTransaction (String date){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("DELETE FROM Transaction WHERE  date_ >= ? AND date_ <= ?");
            preparedStatement.setString(1, monthFront(date));
            preparedStatement.setString(2, monthEnd(date));
            preparedStatement.executeUpdate();
            System.out.println("Deleted Transactions from Transaction Table");
        } catch (SQLException ignored) {
            System.out.println("DeletingTransactions from Transaction Table failed w/ SQLException" + ignored);
            return false;
        }
        System.out.println("Old transaction records deleted successfully");
        return true;
    }
    static boolean isMonthEnd (String date){
        String tail = date.substring(0,4);
        return  tail.equals("0131") || tail.equals("0228") || tail.equals("0331") || tail.equals("0430") ||
                tail.equals("0531") || tail.equals("0630") || tail.equals("0731") || tail.equals("0831") ||
                tail.equals("0930") || tail.equals("1031") || tail.equals("1130") || tail.equals("1231");
    }
    static int getSTOCKbalance (Server server, String sid){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM  Stock_Account WHERE s_acc_id = ?");
            preparedStatement.setString(1, sid);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(3);
            } else {
                return Integer.MIN_VALUE;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when retrieving stock balance" + ignored);
            return Integer.MIN_VALUE;
        }
    }
    boolean generateCustomerReport (String tid){
        String mid = getMARKETACCOUNTNumber(server, tid);
        System.out.println("Beginning of Customer Report");
        System.out.println("|AccountID|Type|Symbol|Balance|");
        System.out.println("|" + mid + "|" + "Market"+ "|" + "---" + "|" + Sformat(getFinalBalance(server, tid, mid, getDate())) + "|");
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM   Has_Account WHERE tax_id = ? AND m_acc_id = ?");
            preparedStatement.setString(1, tid);
            preparedStatement.setString(2, mid);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String sid = resultSet.getString(3);
                int balance = getSTOCKbalance(server, sid);
                String symbol = getSTOCKSymbol(server, sid);
                if (balance != Integer.MIN_VALUE && !symbol.equals("---"))
                    System.out.println("|" + sid + "|" + "Stock" + "|" + symbol + "|" + Integer.toString(balance) + "|");
            }
        } catch (SQLException ignored) {
            System.out.println("Failed login w/ SQLException" + ignored);
            return false;
        }
        System.out.println("End of Customer Report");
        return true;
    }
    Customer queryCustomer(String tid){
        Customer customer = new Customer(tid,"","","","","","");
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Customer WHERE tax_id = ? ");
            preparedStatement.setString(1, tid);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String cname = resultSet.getString(2);
                String state = resultSet.getString(3);
                String phone = resultSet.getString(4);
                String email = resultSet.getString(5);
                String username = resultSet.getString(6);
                String password = resultSet.getString(7);
                customer.cname = cname;
                customer.state = state;
                customer.phone_num = phone;
                customer.email_add = email;
                customer.username = username;
                customer.password = password;
                return customer;
            } else {
                System.out.println("No customer with this tax ID found!");
                return customer;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when querying customer info" + ignored);
            return customer;
        }
    }
    static double getInitialBalance (Server server, String tid, String aid, String date){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Transaction WHERE transaction_id = (SELECT MIN(transaction_id) FROM Transaction WHERE tax_id = ? AND m_acc_id = ? AND date_ >= ? AND date_ <= ?)");
            preparedStatement.setString(1, tid);
            preparedStatement.setString(2, aid);
            preparedStatement.setString(3, monthFront(date));
            preparedStatement.setString(4, monthEnd(date));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return Dformat(resultSet.getDouble(9));
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when fetching initial Market Account balance" + ignored);
        }
        return Double.MIN_VALUE;
    }
    static double getTotalCommission (Server server, String tid, String aid, String date){
        int counter = 0;
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT COUNT(transaction_id) FROM Transaction WHERE tax_id = ? AND m_acc_id = ? AND date_ >= ? AND date_ <= ? AND (type = ? OR type = ? OR type = ?)");
            preparedStatement.setString(1, tid);
            preparedStatement.setString(2, aid);
            preparedStatement.setString(3, monthFront(date));
            preparedStatement.setString(4, monthEnd(date));
            preparedStatement.setString(5, "buy");
            preparedStatement.setString(6, "sell");
            preparedStatement.setString(7, "cancel");
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                counter = resultSet.getInt(1);
            } else
                return Double.MIN_VALUE;
        } catch (SQLException ignored) {
            System.out.println("SQLException when fetching final Market Account balance" + ignored);
            return Double.MIN_VALUE;
        }
        return Dformat(counter * 20.0);
    }
    boolean generateMonthlyStatement (Customer customer){
        System.out.println("Beginning of Customer Statement: " + customer.tax_id);
        System.out.println("Customer's name: " + customer.cname + ", Customer's email: " + customer.email_add);
        System.out.println("Initial balance: " + Sformat(getInitialBalance(server, customer.tax_id, getMARKETACCOUNTNumber(server, customer.tax_id), getDate())) + ", Final balance: " + Sformat(getFinalBalance(server, customer.tax_id, getMARKETACCOUNTNumber(server, customer.tax_id), getDate())));
        System.out.println("Total Earnings: " + Sformat(getTotalEarnings(server, customer.tax_id, getMARKETACCOUNTNumber(server, customer.tax_id), getDate())) + ", Total Commission: " + Sformat(getTotalCommission(server, customer.tax_id, getMARKETACCOUNTNumber(server, customer.tax_id), getDate())));
        System.out.println(" ");
        printOnesAllAccounts(server, customer.tax_id);
        System.out.println("End of Customer Statement: " + customer.tax_id);
        return true;
    }
    static boolean printOnesAllAccounts(Server server, String tid){
        String date = getDate(server);
        if (!printOneAccount(server, tid, getMARKETACCOUNTNumber(server, tid), 0,date)) return false;
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Has_Account WHERE tax_id = ? AND m_acc_id = ?");
            preparedStatement.setString(1, tid);
            preparedStatement.setString(2, getMARKETACCOUNTNumber(server, tid));
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String sid = resultSet.getString(3);
                boolean b2 = printOneAccount(server, tid, sid, 1, date);
                System.out.println(" ");
                if (!b2) return false;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when printing All Accounts' Transactions" + ignored);
            return false;
        }
        return true;
    }
    static String getSTOCKSymbol (Server server, String sid){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Stock_Account WHERE s_acc_id = ?");
            preparedStatement.setString(1, sid);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString(2);
            } else {
                return "---";
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when retrieving stock balance" + ignored);
            return "---";
        }
    }
    static double getFinalBalance (Server server, String tid, String aid, String date){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Transaction WHERE transaction_id = (SELECT MAX(transaction_id) FROM Transaction WHERE tax_id = ? AND m_acc_id = ? AND date_ >= ? AND date_ <= ?)");
            preparedStatement.setString(1, tid);
            preparedStatement.setString(2, aid);
            preparedStatement.setString(3, monthFront(date));
            preparedStatement.setString(4, monthEnd(date));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return Dformat(resultSet.getDouble(9));
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when fetching final Market Account balance" + ignored);
        }
        return Double.MIN_VALUE;
    }
    boolean setDate(String date) {
        String currentTop = getDate();
         System.out.println(date);
         System.out.println(Integer.parseInt(date.substring(4)+date.substring(0,4)) + " " +Integer.parseInt(currentTop.substring(4)+currentTop.substring(0,4)));
        if (Integer.parseInt(date.substring(4)+date.substring(0,4)) <= Integer.parseInt(currentTop.substring(4)+currentTop.substring(0,4))){
            System.out.println("Date to be set must be after current date");
            return false;
        }
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Date_(date_) VALUES (?)");
            preparedStatement.setString(1, date);
            preparedStatement.executeUpdate();
            System.out.println("Date set to: " + date);
        } catch (SQLException e) {
            System.out.println("Set Date failed" + e);
            return false;
        }
        if (!recordAllDailyBalance(server, currentTop)) return false;
        boolean boo2 = true;
        if (isMonthEnd(currentTop))
            boo2 = accureInterestHelper(server, getMonth(currentTop));
        if (!boo2) return false;
        return true;
    }
    boolean setClosingPrice(String stock_symbol, double price){
        if (price == 0) {
            System.out.println("Wrong input for price, try again.");
            return false;
        }
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("UPDATE Stock SET Stock.closing_price = ? WHERE  Stock.stock_symbol = ?");
            preparedStatement.setDouble(1, price);
            preparedStatement.setString(2, stock_symbol);
            preparedStatement.executeUpdate();
            System.out.println("Set " + stock_symbol + " closing price to: " + price);
            return true;
        } catch (SQLException ignored) {
            System.out.println("Setting stock closing price failed w/ SQLException" + ignored);
            return false;
        }
    }
    boolean setStockPrice(String stock_symbol, double price) {
        if (price == 0) {
            System.out.println("Wrong input for price, try again.");
            return false;
        }
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("UPDATE Stock SET Stock.current_price = ? WHERE  Stock.stock_symbol = ?");
            preparedStatement.setDouble(1, price);
            preparedStatement.setString(2, stock_symbol);
            preparedStatement.executeUpdate();
            System.out.println("Set " + stock_symbol + " price to: " + price);
            setClosingPrice(stock_symbol, price);
            return true;
        } catch (SQLException ignored) {
            System.out.println("Setting stock price failed w/ SQLException" + ignored);
            return false;
        }
    }
    static double getTotalEarnings (Server server, String tid, String aid, String date){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT SUM(amount) FROM Transaction WHERE tax_id = ? AND m_acc_id = ? AND date_ >= ? AND date_ <= ? AND (type = ? OR type = ?)");
            preparedStatement.setString(1, tid);
            preparedStatement.setString(2, aid);
            preparedStatement.setString(3, monthFront(date));
            preparedStatement.setString(4, monthEnd(date));
            preparedStatement.setString(5, "stock_earn");
            preparedStatement.setString(6, "interest");
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) 
                return Dformat(resultSet.getDouble(1));
        } catch (SQLException ignored) {
            System.out.println("SQLException when fetching final Market Account balance" + ignored);
        }
        return Double.MIN_VALUE;
    }
    static int getBuyAmount (Server server, String tid, String date){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT SUM(amount) FROM Transaction WHERE tax_id = ? AND m_acc_id = ? AND type = ? AND date_ >= ? AND date_ <= ?");
            preparedStatement.setString(1, tid);
            preparedStatement.setString(2, getMARKETACCOUNTNumber(server, tid));
            preparedStatement.setString(3, "buy");
            preparedStatement.setString(4, monthFront(date));
            preparedStatement.setString(5, monthEnd(date));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when getting buy amount" + ignored);
        }
        return Integer.MIN_VALUE;
    }
    static int getSellAmount (Server server, String tid, String date){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT SUM(amount) FROM Transaction WHERE tax_id = ? AND m_acc_id = ? AND type = ? AND date_ >= ? AND date_ <= ?");
            preparedStatement.setString(1, tid);
            preparedStatement.setString(2, getMARKETACCOUNTNumber(server, tid));
            preparedStatement.setString(3, "sell");
            preparedStatement.setString(4, monthFront(date));
            preparedStatement.setString(5, monthEnd(date));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when getting sell amount" + ignored);
        }
        return Integer.MIN_VALUE;
    }
    boolean generateActiveUserList (){
        System.out.println("Beginning of Active Customer List");
        System.out.println("|TaxID|Name|Amount|");
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM   Customer ");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String tid = resultSet.getString(1);
                if(tid.equals("000001000"))
                    continue;
                int total = hasPassed1000(server, tid, getDate());
                if (total >= 1000)
                    System.out.println("|" + tid+ "|" + resultSet.getString(2) + "|" + Integer.toString(total) + "|");
            }
        } catch (SQLException ignored) {
            System.out.println("Failed login w/ SQLException" + ignored);
            return false;
        }
        System.out.println("End of Active Customer List");
        return true;
    }
    boolean manualAccureInterest(){
        String currentDate = getDate();
        if (!recordAllDailyBalance(server, currentDate)) return false;
        if (!accureInterestHelper(server, getMonth(currentDate))) return false;
        return true;
    }
    static boolean recordAllDailyBalance (Server server, String lastDate){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Market_Account");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String mid = resultSet.getString(1);
                if (mid.equals("000")) continue;
                if (!addDailyBalance (server, mid, lastDate, resultSet.getDouble(2))) return false;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when checking if have bought specific stock before" + ignored);
            return false;
        }
        return true;
    }
    static boolean hasDailyBalanceBefore (Server server, String mid, String lastDate){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Daily_Balance WHERE m_acc_id = ? AND date_ = ?");
            preparedStatement.setString(1, mid);
            preparedStatement.setString(2, lastDate);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when checking if has daily balance before" + ignored);
            return false;
        }
    }
    static boolean addDailyBalance(Server server, String mid, String lastDate, double balance){
        if (hasDailyBalanceBefore(server, mid, lastDate)){
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("UPDATE Daily_Balance SET balance = ? WHERE  m_acc_id = ? AND date_ = ? AND month = ?");
                preparedStatement.setDouble(1, balance);
                preparedStatement.setString(2, mid);
                preparedStatement.setString(3, lastDate);
                preparedStatement.setString(4, lastDate.substring(0,2) + lastDate.substring(4));
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Updating record in Daily_Balance Table failed w/ SQLException" + e);
                return false;
            }
            return true;
        }
        else {
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Daily_Balance VALUES (?, ?, ?, ?)");
                preparedStatement.setString(1, mid);
                preparedStatement.setString(2, lastDate);
                preparedStatement.setString(3, lastDate.substring(0, 2) + lastDate.substring(4));
                preparedStatement.setDouble(4, balance);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Inserting record into Daily_Balance Table failed w/ SQLException" + e);
                return false;
            }
            return true;
        }
    }
    static boolean accureInterestHelper(Server server, String month) {
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT m_acc_id, AVG(balance) FROM Daily_Balance WHERE month = ? GROUP BY m_acc_id ");
            preparedStatement.setString(1, month);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String mid = resultSet.getString(1);
                if (mid.equals("000")) continue;
                if (!addInterest (server, mid, resultSet.getDouble(2) * interest, month)) return false;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when adding interests to accounts" + ignored);
            return false;
        }
        return true;
    }
    static double Dformat(double num){
        return Math.floor(num * 100) / 100;
    }
    static boolean addInterest(Server server, String mid, double amount, String month) {
        amount = Dformat(amount);
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("UPDATE Market_Account market_account SET market_account.balance = market_account.balance + ? WHERE  m_acc_id = ? ");
            preparedStatement.setDouble(1, amount);
            preparedStatement.setString(2, mid);
            preparedStatement.executeUpdate();
            System.out.println("Month: " + month + ", " +  amount + " of interest added to the account: " + mid + ", now having balance: " + getCurrentMARKETBalance(server,mid));
        } catch (SQLException ignored) {
            System.out.println("Adding interest failed w/ SQLException" + ignored);
            return false;
        }
        return uploadTransactionRecord(server, whoOwnsThisMARKETACCOUNT(server,mid), getDate(server),"interest", mid, "---", "---", amount, getCurrentMARKETBalance(server, mid));
    }
    static String getDate(Server server) {
        try {
            ResultSet resultSet = server.connection.prepareStatement("SELECT date_ FROM Date_ ORDER BY id DESC FETCH FIRST 1 ROWS ONLY").executeQuery();
            if (resultSet.next()) {
                String date = resultSet.getString(1);
                if (date == null){
                    System.out.println("null when getting date, returning 011970");
                    return "01011970";
                }
                return date;
            } else {
                System.out.println("No previous date set, returning 011970");
                return "01011970";
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException, returning 011970" + ignored);
            return "01011970";
        }
    }
    static String whoOwnsThisMARKETACCOUNT (Server server, String mid){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Has_Account WHERE m_acc_id= ? ");
            preparedStatement.setString(1, mid);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String tid = resultSet.getString(1);
                if (tid == null){
                    System.out.println("null when getting tid, returning 0000000000");
                    return "0000000000";
                }
                return tid;
            } else {
                System.out.println("Empty when getting tid, returning 0000000000");
                return "0000000000";
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when getting tid, returning 0000000000" + ignored);
            return "0000000000";
        }
    }
    static boolean withdrawMARKETACCOUNT(Server server, String marketAccountID, double amount, String type){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("UPDATE Market_Account market_account SET market_account.balance = market_account.balance - ? WHERE  m_acc_id = ? ");
            preparedStatement.setDouble(1, amount);
            preparedStatement.setString(2, marketAccountID);
            preparedStatement.executeUpdate();
            if (type.contains("stock")) 
                return true;
            else {
                System.out.println("Withdraw succeeded");
                return true;
            }
        } catch (SQLException ignored) {
            System.out.println("Withdraw failed w/ SQLException" + ignored);
            return false;
        }
    }
    static boolean hasThousandFlag (Server server, String marketAccountID){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Market_Account WHERE  m_acc_id = ? ");
            preparedStatement.setString(1, marketAccountID);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                int flag = resultSet.getInt(3);
                return flag == 1;
            } else {
                System.out.println("No market account with ID: " + marketAccountID);
                return true;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when checking thousand flag of market account with ID: " + marketAccountID);
            return true;
        }
    }
    boolean getTransactionHistory (Customer customer){
        System.out.println("Beginning of Transaction History Report");
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Has_Account WHERE tax_id = ? AND m_acc_id = ?");
            preparedStatement.setString(1, customer.tax_id);
            preparedStatement.setString(2, getMARKETACCOUNTNumber(server, customer.tax_id));
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                if (!printOneAccount(server, customer.tax_id, resultSet.getString(3), -1, "")) return false;
                System.out.println(" ");
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when printing All Stock Accounts' Transactions" + ignored);
            return false;
        } finally {
            System.out.println("End of Transaction History Report");
        }
        return true;
    }
    static String monthFront(String date){
        if (date != null && date.length() == 8)
            return date.substring(0, 2) + "01" + date.substring(4);
        else
            return "";
    }
    static String monthEnd(String date){
        if (date != null && date.length() == 8) {
            String month = date.substring(0,2);
            if (month.equals("01")) return month + "31" + date.substring(4);
            if (month.equals("02")) return month + "28" + date.substring(4);
            if (month.equals("03")) return month + "31" + date.substring(4);
            if (month.equals("04")) return month + "30" + date.substring(4);
            if (month.equals("05")) return month + "31" + date.substring(4);
            if (month.equals("06")) return month + "30" + date.substring(4);
            if (month.equals("07")) return month + "31" + date.substring(4);
            if (month.equals("08")) return month + "31" + date.substring(4);
            if (month.equals("09")) return month + "30" + date.substring(4);
            if (month.equals("10")) return month + "31" + date.substring(4);
            if (month.equals("11")) return month + "30" + date.substring(4);
            if (month.equals("12")) return month + "31" + date.substring(4);
            return "";
        }
        else
            return "";
    }
    boolean getMovieReviews (String title){
        System.out.println("Beginning of Movie Review of " + title);
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Review WHERE title = ?");
            preparedStatement.setString(1, title);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                System.out.println("Movie: " + title + " (" + resultSet.getString(3) + ") Rank: " +resultSet.getString(5));
                System.out.println("Review: " + resultSet.getString(4));
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when printing All Movie info"+ ignored);
            return false;
        } finally {
            System.out.println("End of Movie Review of " + title);
        }
        return true;
    }
    static String getMonth(String date) {
        if (date != null && date.length() == 8)
            return date.substring(0,2)+date.substring(4);
        else
            return "";
    }
    boolean withdraw(Customer customer, double amount, String type) {
        if (amount < 0){
            System.out.println("Withdraw amount must > 0!");
            return false;
        }
        
        String marketAccountID = getMARKETACCOUNTNumber(server, customer.tax_id);
        if (marketAccountID.equals("000")) {
            System.out.println("Must have market account before withdraw!");
            return false;
        }
        if (hasThousandFlag(server, marketAccountID)) {
            System.out.println("Must deposit $1000 to activate market account before other transaction");
            return false;
        }
        
        double balanceBefore = getCurrentMARKETBalance(server, marketAccountID);
        if (balanceBefore == Double.MIN_VALUE) return false;
        if ((balanceBefore - amount) < 0) {
            System.out.println("Over-withdrawing from market account! Balance would be negative!");
            return false;
        }
        
        if (withdrawMARKETACCOUNT(server, marketAccountID, amount, type) == false) return false;
        
        double balanceAfter = getCurrentMARKETBalance(server, marketAccountID);
        if (balanceAfter == Double.MIN_VALUE) return false;
        
        if (uploadTransactionRecord(server, customer.tax_id, getDate(), type, marketAccountID, "---", "---", -amount, getCurrentMARKETBalance(server, marketAccountID)) == false) return false;
        System.out.println("Market account with taxID: " + customer.tax_id +", amount of " + amount + " withdrawn, current balance: " + balanceAfter);
        return true;
    }
    static double getShare (Server server, String taxID, String stock_symbol){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT h.tax_id, s.stock_symbol, s.share_ FROM  (Has_Account h INNER JOIN Stock_Account s ON h.s_acc_id = s.s_acc_id) WHERE h.tax_id = ? AND s.stock_symbol = ?");
            preparedStatement.setString(1, taxID);
            preparedStatement.setString(2, stock_symbol);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getDouble(3);
            } else {
                return Double.MIN_VALUE;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when checking if have bought specific stock before" + ignored);
            return Double.MIN_VALUE;
        }
    }
    boolean buyStock(Customer customer, String stock_symbol, double amount) {
        if (amount < 0){
            System.out.println("Share purchasing amount must > 0!");
            return false;
        }
        
        String marketAccountID = getMARKETACCOUNTNumber(server, customer.tax_id);
        if (marketAccountID.equals("000")) {
            System.out.println("Must have market account before buying stock!");
            return false;
        }
        if (hasThousandFlag(server, marketAccountID)) {
            System.out.println("Must deposit $1000 to activate market account before other transaction");
            return false;
        }
        
        double current_price = getStockPrice(server, stock_symbol);
        if (current_price == Double.MIN_VALUE) {
            System.out.println("No such stock found!");
            return false;
        }
        double total = 20 + amount * current_price;
        
        double balanceBefore = getCurrentMARKETBalance(server, marketAccountID);
        if ((balanceBefore - total) < 0) {
            System.out.println("Market balance will be below 0 after buying stock!");
            return false;
        }
        
        if (buyStockSTOCKACCOUNT(server, customer.tax_id, stock_symbol, amount) == false) return false;
        
        if (withdraw(customer, total, "stock_deduct") == false) return false;
        
        if (uploadTransactionRecord(server, customer.tax_id, getDate(), "buy", marketAccountID, getSpecificStockAccountID(server, customer.tax_id, stock_symbol), stock_symbol, amount, getCurrentMARKETBalance(server, marketAccountID)) == false) return false;
        System.out.println("TaxID: " + customer.tax_id + " bought " + amount + " shares of " + stock_symbol + " stock, now having market balance: " + (balanceBefore - total));
        setClosingPrice(stock_symbol, current_price);
        return true;
    }
    static boolean unsetThousandFlag (Server server, String marketAccountID){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("UPDATE Market_Account market_account SET market_account.thousand_flag = ? WHERE  m_acc_id = ? ");
            preparedStatement.setInt(1, 0);
            preparedStatement.setString(2, marketAccountID);
            preparedStatement.executeUpdate();
            System.out.println("Unset thousand flag for market account: " + marketAccountID);
            return true;
        } catch (SQLException ignored) {
            System.out.println("Unset thousand flag failed w/ SQLException"+ignored);
            return false;
        }
    }
    static boolean printOneAccount(Server server, String tid, String aid, int type, String date){
        if (type == 0) { 
            System.out.println("Market Account Number: " + aid + " Transaction Records of Month: " + getMonth(date));
            System.out.println("|Transaction ID|date|type|amount|");
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Transaction WHERE tax_id = ? AND m_acc_id = ? AND stock_symbol = ? AND date_ >= ? AND date_ <= ?");
                preparedStatement.setString(1, tid);
                preparedStatement.setString(2, aid);
                preparedStatement.setString(3, "---");
                preparedStatement.setString(4, monthFront(date));
                preparedStatement.setString(5, monthEnd(date));
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    System.out.println("|" + resultSet.getString(1)+ "|" + resultSet.getString(3) + "|" + resultSet.getString(4) + "|" + Sformat(Math.abs(Double.parseDouble(resultSet.getString(8)))) + "|");
                }
                System.out.println(" ");
            } catch (SQLException ignored) {
                System.out.println("SQLException when printing Market Account Transactions" + ignored);
                return false;
            }
        }
        else if (type == -1){
            System.out.println( "Stock Account Number: " + aid + "'s All Transaction Records");
            System.out.println("|Transaction ID|date|type|symbol|amount|");
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Transaction WHERE tax_id = ? AND s_acc_id = ?");
                preparedStatement.setString(1, tid);
                preparedStatement.setString(2, aid);
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    System.out.println("|"+resultSet.getString(1) + "|" + resultSet.getString(3) + "|" + resultSet.getString(4) + "|" + resultSet.getString(7)+ "|" + Sformat(Math.abs(Double.parseDouble(resultSet.getString(8)))) + "|");
                }
            } catch (SQLException ignored) {
                System.out.println("SQLException when printing Stock Account Transactions" + ignored);
                return false;
            }
        }
        else {  
            System.out.println("Stock Account Number: " + aid + ", Transaction Records of Month: " + getMonth(date));
            System.out.println("|Transaction ID|date|type|symbol|amount|");
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Transaction WHERE tax_id = ? AND s_acc_id = ? AND date_ >= ? AND date_ <= ?");
                preparedStatement.setString(1, tid);
                preparedStatement.setString(2, aid);
                preparedStatement.setString(3, monthFront(date));
                preparedStatement.setString(4, monthEnd(date));
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    System.out.println("|"+ resultSet.getString(1) + "|" + resultSet.getString(3) + "|" + resultSet.getString(4) + "|" + resultSet.getString(7)+ "|" + Sformat(Math.abs(Double.parseDouble(resultSet.getString(8)))) + "|");
                }
            } catch (SQLException ignored) {
                System.out.println("SQLException when printing Stock Account Transactions" + ignored);
                return false;
            }
        }
        return true;
    }
    static boolean sellStockSTOCKACCOUNT (Server server, String taxID, String stock_symbol, double amount){
        if (getShare(server, taxID, stock_symbol) - amount >= 0) {
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("UPDATE Stock_Account stock_account SET stock_account.share_ = stock_account.share_ - ? WHERE  s_acc_id = ? AND stock_symbol = ? ");
                preparedStatement.setDouble(1, amount);
                preparedStatement.setString(2, getSpecificStockAccountID(server, taxID, stock_symbol));
                preparedStatement.setString(3, stock_symbol);
                preparedStatement.executeUpdate();
                System.out.println("Stock sell succeeded");
            } catch (SQLException ignored) {
                System.out.println("Stock sell failed w/ SQLException" + ignored);
                return false;
            }
        }
        else {
            System.out.println("Cannot sell more share than you own!");
            return false;
        }
        return true;
    }
    boolean topMovies (String timeFront, String timeEnd){
        System.out.println("Beginning of Top Movie List");
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Movie WHERE rank = ? AND year >= ? AND year <= ?");
            preparedStatement.setString(1, "10.0");
            preparedStatement.setString(2, timeFront);
            preparedStatement.setString(3, timeEnd);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                System.out.println("Movie: " + resultSet.getString(1) + " (" + resultSet.getString(2) + ") \n Rank: " +resultSet.getString(3));
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when printing All Movie info" + ignored);
            return false;
        } finally {
            System.out.println("End of Top Movie List");
        }
        return true;
    }
    boolean getOneMovie (String title){
        System.out.println("Beginning of Movie Info");
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Movie WHERE title = ?");
            preparedStatement.setString(1,title);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                System.out.println("Movie: " + title + " (" + resultSet.getString(2) + ") Rank: " +resultSet.getString(3));
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when printing One Movie info" + ignored);
            return false;
        } finally {
            System.out.println("End of Movie Info");
        }
        return true;
    }
    static boolean checkNONAPPLICABLEAccount (Server server, int select){
        String query;
        if (select == 1)
            query = "SELECT * FROM Market_Account WHERE  m_acc_id = ? ";
        else
            query = "SELECT * FROM Stock_Account WHERE  s_acc_id = ? ";
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement(query);
            preparedStatement.setString(1, "000");
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String id = resultSet.getString(1);
                if (id == null) {
                    System.out.println("id equals null when checking if has previous \"000\" return false");
                    return false;
                }
                return true;
            } else {
                System.out.println("account not found when checking if has previous \"000\" return false");
                return false;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when checking if has previous \"000\" return false" + ignored);
            return false;
        }
    }
    static boolean checkNONAPPLICABLEStock (Server server){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Stock WHERE  stock_symbol = ? ");
            preparedStatement.setString(1, "---");
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString(1) != null;
            } else
                return false;
        } catch (SQLException ignored) {
            return false;
        }
    }
    static boolean checkDashccount (Server server){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM  Stock_Account sWHERE  s_acc_id = ? ");
            preparedStatement.setString(1, "---");
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                if (resultSet.getString(1) == null) {
                    System.out.println("id equals null when checking if has previous \"---\" return false");
                    return false;
                }
                return true;
            } else {
                System.out.println("account not found when checking if has previous \"---\" return false");
                return false;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when checking if has previous \"---\" return false" + ignored);
            return false;
        }
    }
    static boolean createNONAPPLICABLEAccounts(Server server) {
        boolean hasNONAPPLICABLEMTK = checkNONAPPLICABLEAccount(server, 1);
        boolean hasNONAPPLICABLESTOCK = checkNONAPPLICABLEAccount(server, 2);
        boolean hasNONAPPLICABLESTOCK2 = checkNONAPPLICABLEStock(server);
        boolean hasDash = checkDashccount(server);
        if (hasNONAPPLICABLEMTK && hasNONAPPLICABLESTOCK && hasNONAPPLICABLESTOCK2 && hasDash) {
            return true;
        }
        if (!hasNONAPPLICABLEMTK){
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Market_Account VALUES (?, ?, ?)");
                preparedStatement.setString(1, "000");
                preparedStatement.setDouble(2, 0);
                preparedStatement.setInt(3, 0);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Inserting \"000\" into Market_Account Table failed w/ SQLException" + e);
                return false;
            }
        }
        if (!hasNONAPPLICABLESTOCK){
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Stock_Account VALUES (?, ?, ?)");
                preparedStatement.setString(1, "000");
                preparedStatement.setString(2, "---");
                preparedStatement.setDouble(3, 0);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Inserting \"000\" into Stock_Account Table failed w/ SQLException" + e);
                return false;
            }
        }
        if (!checkNONAPPLICABLEStock(server)){
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Stock VALUES (?, ?, ?)");
                preparedStatement.setString(1, "---");
                preparedStatement.setDouble(2, 0);
                preparedStatement.setDouble(3, 0);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Inserting \"---\" into Stock Table failed w/ SQLException" + e);
                return false;
            }
        }
        if (!checkDashccount(server)){
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Stock_Account VALUES (?, ?, ?)");
                preparedStatement.setString(1, "---");
                preparedStatement.setString(2, "---");
                preparedStatement.setDouble(3, 0);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Inserting \"---\" into Stock_Account Table failed w/ SQLException");
                return false;
            }
        }
        System.out.println("All \"000\" \"NONAPPLICABLE\" accounts inserted into Market & Stock Account Tables");
        return true;
    }
    static boolean uploadTransactionRecord(Server server, String tax_id, String date, String type, String m_acc_id, String s_acc_id, String stock_symbol, double amount, double current_balance) {
        System.out.println(tax_id + " " + date + " " + type + " " + m_acc_id + " " + s_acc_id + " " + stock_symbol + " " + amount + " " + current_balance);
        String nextTransId = getNextTransID(server);
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Transaction VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
            preparedStatement.setString(1, nextTransId);
            preparedStatement.setString(2, tax_id);
            preparedStatement.setString(3, date);
            preparedStatement.setString(4, type);
            preparedStatement.setString(5, m_acc_id);
            preparedStatement.setString(6, s_acc_id);
            preparedStatement.setString(7, stock_symbol);
            preparedStatement.setDouble(8, amount);
            preparedStatement.setDouble(9, current_balance);
            preparedStatement.executeUpdate();
            System.out.println("Transaction record: " + nextTransId + " inserted");
            return true;
        } catch (SQLException e) {
            System.out.println("Transaction record insertion failed" + e);
        }
        return false;
    }
    boolean login(Customer customer) {
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM   Customer WHERE  username = ? AND password = ? ");
            preparedStatement.setString(1, customer.username);
            preparedStatement.setString(2, customer.password);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                customer.tax_id = resultSet.getString(1);
                customer.cname = resultSet.getString(2);
                customer.state = resultSet.getString(3);
                customer.phone_num = resultSet.getString(4);
                customer.email_add = resultSet.getString(5);
                System.out.println("Login succeeded");
                return true;
            } else {
                System.out.println("Invalid email/passcode combination");
                return false;
            }
        } catch (SQLException ignored) {
            System.out.println("Failed login w/ SQLException" + ignored);
        }
        return false;
    }
    boolean getBalance(Customer customer){
        double balance = getCurrentMARKETBalance(server, getMARKETACCOUNTNumber(server, customer.tax_id));
        if (balance != Double.MIN_VALUE){
            System.out.println("Market Account Balance for customer " + customer.cname + " is: " + Sformat(balance));
            return true;
        }
        else{
            System.out.println("Failed to retrieve balance!");
            return false;
        }
    }
    static String Sformat(double num){
        return String.format( "%.2f", num );
    }
    static double getCurrentMARKETBalance(Server server, String marketAccountID){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM   Market_Account WHERE  m_acc_id = ? ");
            preparedStatement.setString(1, marketAccountID);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getDouble(2);
            } else {
                System.out.println("Failed retrieving current balance");
                return Double.MIN_VALUE;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when retrieving current balance" + ignored);
            return Double.MIN_VALUE;
        }
    }
    String getDate() {
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT date_ FROM Date_ ORDER BY id DESC FETCH FIRST 1 ROWS ONLY");
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String date = resultSet.getString(1);
                if (date == null){
                    System.out.println("null when getting date, returning 011970");
                    return "01011970";
                }
                return date;
            } else {
                System.out.println("No previous date set, returning 011970");
                return "01011970";
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException, returning 011970" + ignored);
            return "01011970";
        }
    }
    static String getMARKETACCOUNTNumber(Server server, String tax_id) {
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM   Has_Account WHERE  tax_id = ? ");
            preparedStatement.setString(1, tax_id);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String accNum = resultSet.getString(2);
                if (accNum == null) {
                    System.out.println("Market account with taxID: " + tax_id + " doesn't exist, but has Stock account");
                    return "000";
                }
                else
                    return accNum;
            } else {
                System.out.println("Market account with taxID: " + tax_id + " doesn't exist");
                return "000";
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when fetching market account ID" + ignored);
            return "000";
        }
    }
    static String getNextMARKETACCOUNTNumber(Server server) {
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT MAX(m_acc_id) FROM   Market_Account ");
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String currentID = resultSet.getString(1);
                if (currentID == null) {
                    System.out.println("Previous market account ID returning null or 000, this would be first account ID");
                    return "001";
                }
                String result = Integer.toString(Integer.parseInt(currentID) + 1);
                while (result.length() != 3) {
                    result = "0" + result;
                }
                return result;
            } else {
                System.out.println("No previous market account ID, this would be first account ID");
                return "001";
            }
        } catch (SQLException ignored) {
            System.out.println("No previous market account ID, this would be first account ID" + ignored);
            return "001";
        }
    }
    static boolean openMARKETACCOUNT (Server server, String taxID){
        createNONAPPLICABLEAccounts(server);
        if (!getMARKETACCOUNTNumber(server, taxID).equals("000")){
            System.out.println("Already have a market account associated with this taxID");
            return false;
        }
        String marketAccountID = getNextMARKETACCOUNTNumber(server);
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Market_Account VALUES (?, ?, ?)");
            preparedStatement.setString(1, marketAccountID);
            preparedStatement.setDouble(2, 0);
            preparedStatement.setInt(3, 1);
            preparedStatement.executeUpdate();
            System.out.println("New market account inserted into Market_Account Table, account ID: " + marketAccountID);
        } catch (SQLException e) {
            System.out.println("Inserting into Market_Account Table failed w/ SQLException" + e);
            return false;
        }
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Has_Account VALUES (?, ?, ?)");
            preparedStatement.setString(1, taxID);
            preparedStatement.setString(2, marketAccountID);
            preparedStatement.setString(3, "000");
            preparedStatement.executeUpdate();
            System.out.println("New market account inserted into Has_Account Table, account ID: " + marketAccountID);
        } catch (SQLException e) {
            System.out.println("Inserting into Has_Account Table failed w/ SQLException" + e);
            return false;
        }
        return true;
    }
    boolean signup(String tax_id, String cname, String state, String phone_num, String email_add, String username, String password) {
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Customer VALUES (?, ?, ?, ?, ?, ?, ?)");
            preparedStatement.setString(1, tax_id);
            preparedStatement.setString(2, cname);
            preparedStatement.setString(3, state);
            preparedStatement.setString(4, phone_num);
            preparedStatement.setString(5, email_add);
            preparedStatement.setString(6, username);
            preparedStatement.setString(7, password);
            preparedStatement.executeUpdate();
            System.out.println("Registration succeeded, creating market account...");
        } catch (SQLException e) {
            System.out.println("Registration failed w/ SQLException" + e);
            return false;
        }
        if (openMARKETACCOUNT(server, tax_id)){
            System.out.println("New market account created successfully");
            return true;
        }
        else {
            System.out.println("Unable to create market account");
            return false;
        }
    }
    static double getStockPrice(Server server, String stock_symbol) {
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM   Stock WHERE  stock_symbol = ? ");
            preparedStatement.setString(1, stock_symbol);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getDouble(3);
            } else {
                System.out.println("Failed retrieving current stock price of " + stock_symbol);
                return Double.MIN_VALUE;
            }
        } catch (SQLException ignored) {
            System.out.println("Failed retrieving current stock price of [" + stock_symbol + "]" + ignored);
            return Double.MIN_VALUE;
        }
    }
    static String getNextTransID(Server server) {
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT MAX(transaction_id) FROM   Transaction ");
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String currentID = resultSet.getString(1);
                if (currentID == null) {
                    System.out.println("Previous transaction record returning null, this would be first record");
                    return "0000000001";
                }
                String result = Integer.toString(Integer.parseInt(currentID) + 1);
                while (result.length() != 10) {
                    result = "0" + result;
                }
                return result;
            } else {
                System.out.println("No previous transaction record, this would be first record");
                return "0000000001";
            }
        } catch (SQLException ignored) {
            System.out.println("No previous transaction record, this would be first record" + ignored);
            return "0000000001";
        }
    }
    static boolean depositMARKETACCOUNT(Server server, String marketAccountID, double amount, String type) {
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("UPDATE Market_Account market_account SET market_account.balance = market_account.balance + ? WHERE  m_acc_id = ? ");
            preparedStatement.setDouble(1, amount);
            preparedStatement.setString(2, marketAccountID);
            preparedStatement.executeUpdate();
            if (type.contains("stock")) {
                return true;
            } else {
                System.out.println("Deposit succeeded");
                return true;
            }
        } catch (SQLException ignored) {
            System.out.println("Deposit failed w/ SQLException" + ignored);
            return false;
        }
    }
    boolean cancelTrans(Customer customer, String date){
        String marketAccountID = getMARKETACCOUNTNumber(server, customer.tax_id);
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Transaction WHERE transaction_id = (SELECT MAX(transaction_id) FROM Transaction WHERE tax_id = ? AND m_acc_id = ? AND date_ >= ? AND date_ <= ?)");
            preparedStatement.setString(1, customer.tax_id);
            preparedStatement.setString(2, marketAccountID);
            preparedStatement.setString(3, monthFront(date));
            preparedStatement.setString(4, monthEnd(date));
            ResultSet resultSet = preparedStatement.executeQuery();
            if(!resultSet.next()){
                System.out.println("You didn't make any transactions today");
                return false;
            }
            if(resultSet.getString(4).equals("buy")||resultSet.getString(4).equals("sell")){
                System.out.println("latest transaction is buy or sell");
                try {
                    PreparedStatement preparedStatement1 = server.connection.prepareStatement("SELECT * FROM Transaction WHERE transaction_id = (SELECT (MAX(transaction_id) - 1) FROM Transaction WHERE tax_id = ? AND m_acc_id = ? AND date_ >= ? AND date_ <= ?)");
                    preparedStatement1.setString(1, customer.tax_id);
                    preparedStatement1.setString(2, marketAccountID);
                    preparedStatement1.setString(3, monthFront(date));
                    preparedStatement1.setString(4, monthEnd(date));
                    ResultSet resultSet1 = preparedStatement1.executeQuery();
                    if(!resultSet1.next()){
                        System.out.println("Pair 1 of transaction not found");
                        return false;
                    }
                    double total = resultSet1.getDouble(8) + 20;
                    System.out.println(total);
                    if(total>=0.0){
                        if (getCurrentMARKETBalance(server, marketAccountID) - total <0) {
                            System.out.println("Market balance will be negative after cancelling! Because of the cancellation fee!");
                            return false;
                        }
                        if (withdraw(customer, total, "stock_cancel") == false) return false;
                        if (uploadTransactionRecord(server, customer.tax_id, getDate(), "cancel", marketAccountID, resultSet.getString(6), resultSet.getString(7), -resultSet.getDouble(8), getCurrentMARKETBalance(server, marketAccountID)) == false) return false;
                        System.out.println("latest transaction cancelled");
                        return true;
                    }
                    else{
                        if (deposit(customer, -total, "stock_cancel") == false) return false;
                        if (uploadTransactionRecord(server, customer.tax_id, getDate(), "cancel", marketAccountID, resultSet.getString(6), resultSet.getString(7), -resultSet.getDouble(8), getCurrentMARKETBalance(server, marketAccountID)) == false) return false;
                        System.out.println("latest transaction cancelled");
                        return true;
                    }
                }
                catch(SQLException ignored){
                    System.out.println("SQLException when trying to cancel" + ignored);
                }
                return true;
            }
            else{
                System.out.println("Cannot cancel, latest transaction is not buy or sell");
                return false;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when fetching latest transaction" + ignored);
        }
        return false;
    }
    boolean sellStock(Customer customer, String stock_symbol, double original_price, double amount) {
        if (amount < 0){
            System.out.println("Share selling amount must > 0!");
            return false;
        }
        String marketAccountID = getMARKETACCOUNTNumber(server, customer.tax_id);
        if (marketAccountID.equals("000")) {
            System.out.println("Must have market account before selling stock!");
            return false;
        }
        if (hasThousandFlag(server, marketAccountID)) {
            System.out.println("Must deposit $1000 to activate market account before other transaction");
            return false;
        }
        String stockAccountID = getSpecificStockAccountID(server, customer.tax_id, stock_symbol);
        if (stockAccountID.equals("000")) {
            System.out.println("No such stock found!");
            return false;
        }
        if (!hasBoughtStockSymbolBefore(server, customer.tax_id, stock_symbol)) {
            System.out.println("Must have bought this stock before selling stock!");
            return false;
        }
        if (getShare(server, customer.tax_id, stock_symbol) < amount){
            System.out.println("Cannot sell more share than you own!");
            return false;
        }
        double current_price = getStockPrice(server, stock_symbol);
        if (current_price == Double.MIN_VALUE) return false;
        double total = amount * (current_price - original_price) - 20;
        double balanceBefore = getCurrentMARKETBalance(server, marketAccountID);
        if (total < 0) {
            if ((balanceBefore + total) < 0) {
                System.out.println("Market balance will be negative after selling stock! Because this is a loss!");
                return false;
            }
            else{
                if (sellStockSTOCKACCOUNT(server, customer.tax_id, stock_symbol, amount) == false) return false;
                if (withdraw(customer, -total, "stock_loss") == false) return false;
                if (uploadTransactionRecord(server, customer.tax_id, getDate(), "sell", marketAccountID, stockAccountID, stock_symbol, -amount, getCurrentMARKETBalance(server, marketAccountID)) == false) return false;
                System.out.println("TaxID: " + customer.tax_id + " sold " + amount + " shares of " + stock_symbol + " stock, loss is: " + (-total) + ", now having market balance: " + (balanceBefore + total));
                setClosingPrice(stock_symbol, original_price);
                return true;
            }
        }
        else if (total > 0){
            if (sellStockSTOCKACCOUNT(server, customer.tax_id, stock_symbol, amount) == false) return false;
            if (deposit(customer, total + original_price * amount, "stock_earn") == false) return false;
            if (uploadTransactionRecord(server, customer.tax_id, getDate(), "sell", marketAccountID, stockAccountID, stock_symbol, -amount, getCurrentMARKETBalance(server, marketAccountID)) == false) return false;
            System.out.println("TaxID: " + customer.tax_id + " sold " + amount + " shares of " + stock_symbol + " stock, gain is: " + total + ", now having market balance: " + (balanceBefore + total));
            setClosingPrice(stock_symbol, original_price);
            return true;
        }
        else {
            if (sellStockSTOCKACCOUNT(server, customer.tax_id, stock_symbol, amount) == false) return false;
            if (uploadTransactionRecord(server, customer.tax_id, getDate(), "sell", marketAccountID, stockAccountID, stock_symbol, -amount, getCurrentMARKETBalance(server, marketAccountID)) == false) return false;
            System.out.println("TaxID: " + customer.tax_id + " sold " + amount + " shares of " + stock_symbol + " stock, no gain or loss"  + ", now having market balance: " + (balanceBefore));
            setClosingPrice(stock_symbol, original_price);
            return true;
        }
    }
    boolean getAllMovies (){
        System.out.println("Beginning of Movie List");
        try {
            ResultSet resultSet = server.connection.prepareStatement("SELECT * FROM Movie ").executeQuery();
            while (resultSet.next()) {
                System.out.println("Movie: " + resultSet.getString(1) + " (" + resultSet.getString(2) + ")");
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when printing All Movie info" + ignored);
            return false;
        } finally {
            System.out.println("End of Movie List");
        }
        return true;
    }
    boolean getStockInfo (String stock_symbol){
        System.out.println("Beginning of Stock Info for " + stock_symbol);
        double price = getStockPrice(server, stock_symbol);
        if (price==Double.MIN_VALUE)
            System.out.println("No such stock found!");
        System.out.println("Current stock price: " + price);
        String aname = "";
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Actor WHERE stock_symbol = ?");
            preparedStatement.setString(1, stock_symbol);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                aname = resultSet.getString(2);
                System.out.println("Actor name: " + aname + " Date of Birth: " + resultSet.getString(3));
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when printing Stock info" + ignored);
            return false;
        }
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM Has_Contract WHERE stock_symbol = ? AND aname = ?");
            preparedStatement.setString(1, stock_symbol);
            preparedStatement.setString(2, aname);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                System.out.println("Has Contract with Moive: " + resultSet.getString(3)+ " (" + resultSet.getString(4) + ")");
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when printing Stock info");
            return false;
        } finally {
            System.out.println("End of Stock Info for " + stock_symbol);
        }
        return true;
    }
    static boolean hasBoughtStockSymbolBefore (Server server, String taxID, String stock_symbol){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT h.tax_id, s.stock_symbol, s.share_ FROM  (Has_Account h INNER JOIN Stock_Account s ON h.s_acc_id = s.s_acc_id) WHERE h.tax_id = ? AND s.stock_symbol = ?");
            preparedStatement.setString(1, taxID);
            preparedStatement.setString(2, stock_symbol);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when checking if have bought specific stock before" + ignored);
            return false;
        }
    }
    static String getNextSTOCKACCOUNTNumber(Server server) {
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT MAX(s_acc_id) FROM   Stock_Account ");
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String currentID = resultSet.getString(1);
                if (currentID == null) {
                    System.out.println("Previous stock account ID returning null, this would be first account ID");
                    return "001";
                }
                String result = Integer.toString(Integer.parseInt(currentID) + 1);
                while (result.length() != 3) {
                    result = "0" + result;
                }
                return result;
            } else {
                System.out.println("No previous stock account ID, this would be first account ID");
                return "001";
            }
        } catch (SQLException ignored) {
            System.out.println("No previous market stock ID, this would be first account ID" + ignored);
            return "001";
        }
    }
    static boolean hasHasAccountEntry(Server server, String taxID, String mid, String sid){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT * FROM  Has_Account WHERE tax_id = ? AND m_acc_id = ? AND s_acc_id = ?");
            preparedStatement.setString(1, taxID);
            preparedStatement.setString(2, mid);
            preparedStatement.setString(3, sid);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String tid = resultSet.getString(1);
                return tid.equals(taxID);
            } else {
                return false;
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when checking if have bought specific stock before" + ignored);
            return false;
        }
    }
    static boolean buyStockSTOCKACCOUNT (Server server, String taxID, String stock_symbol, double amount){
        if (!hasBoughtStockSymbolBefore(server, taxID, stock_symbol)) {
            String mid = getMARKETACCOUNTNumber(server, taxID);
            String sid = getNextSTOCKACCOUNTNumber(server);
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Stock_Account VALUES (?, ?, ?)");
                preparedStatement.setString(1, sid);
                preparedStatement.setString(2, stock_symbol);
                preparedStatement.setDouble(3, amount);
                preparedStatement.executeUpdate();
                System.out.println("Stock purchase succeeded");
            } catch (SQLException e) {
                System.out.println("Stock purchase failed w/ SQLException");
                return false;
            }
            if (hasHasAccountEntry(server, taxID, mid, "000")){
                try {
                    PreparedStatement preparedStatement = server.connection.prepareStatement("UPDATE Has_Account has_account SET has_account.s_acc_id = ? WHERE tax_id  = ? AND m_acc_id = ? AND s_acc_id = ?");
                    preparedStatement.setString(1, sid);
                    preparedStatement.setString(2, taxID);
                    preparedStatement.setString(3, mid);
                    preparedStatement.setString(4, "000");
                    preparedStatement.executeUpdate();
                    System.out.println("Updated hasAccount stockID to new stockID");
                } catch (SQLException ignored) {
                    System.out.println("Updating hasAccount stockID failed w/ SQLException" + ignored);
                    return false;
                }
            }
            else {
                try {
                    PreparedStatement preparedStatement = server.connection.prepareStatement("INSERT INTO Has_Account VALUES (?, ?, ?)");
                    preparedStatement.setString(1, taxID);
                    preparedStatement.setString(2, mid);
                    preparedStatement.setString(3, sid);
                    preparedStatement.executeUpdate();
                    System.out.println("Inserted new stockID as new row into hasAccount");
                } catch (SQLException e) {
                    System.out.println("Inserting new stockID as new row into hasAccount failed w/ SQLException" + e);
                    return false;
                }
            }
        }
        else {
            try {
                PreparedStatement preparedStatement = server.connection.prepareStatement("UPDATE Stock_Account stock_account SET stock_account.share_ = stock_account.share_ + ? WHERE  s_acc_id = ? AND stock_symbol = ? ");
                preparedStatement.setDouble(1, amount);
                preparedStatement.setString(2, getSpecificStockAccountID(server, taxID, stock_symbol));
                preparedStatement.setString(3, stock_symbol);
                preparedStatement.executeUpdate();
                System.out.println("Stock purchase succeeded");
            } catch (SQLException ignored) {
                System.out.println("Stock purchase failed w/ SQLException" + ignored);
                return false;
            }
        }
        return true;
    }
    static String getSpecificStockAccountID (Server server, String taxID, String stock_symbol){
        try {
            PreparedStatement preparedStatement = server.connection.prepareStatement("SELECT h.tax_id, s.stock_symbol, s.share_, s.s_acc_id FROM  (Has_Account h INNER JOIN Stock_Account s ON h.s_acc_id = s.s_acc_id) WHERE h.tax_id = ? AND s.stock_symbol = ?");
            preparedStatement.setString(1, taxID);
            preparedStatement.setString(2, stock_symbol);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString(4);
            } else {
                return "000";
            }
        } catch (SQLException ignored) {
            System.out.println("SQLException when checking if have bought specific stock before" + ignored);
            return "000";
        }
    }
    boolean deposit(Customer customer, double amount, String type) {
        boolean thousandFlag = false;
        if (amount <= 0){
            System.out.println("Deposit amount must > 0!");
            return false;
        }
        String marketAccountID = getMARKETACCOUNTNumber(server, customer.tax_id);
        if (marketAccountID.equals("000")) {
            System.out.println("Must have market account before deposit!");
            return false;
        }
        if (hasThousandFlag(server, marketAccountID))
            thousandFlag = true;
        if (depositMARKETACCOUNT(server, marketAccountID, amount, type) == false) return false;
        double balanceAfter = getCurrentMARKETBalance(server, marketAccountID);
        if (balanceAfter == Double.MIN_VALUE) return false;
        if (uploadTransactionRecord(server, customer.tax_id, getDate(), type, marketAccountID, "---", "---", amount, getCurrentMARKETBalance(server, marketAccountID)) == false) return false;
        if ((balanceAfter >= 1000 && thousandFlag == true) || (amount >= 1000 && thousandFlag == true))
            unsetThousandFlag (server, marketAccountID);
        System.out.println("Market account with taxID: " + customer.tax_id + ", deposit of " + amount + " added, current balance: " + balanceAfter);
        return true;
    }
}
