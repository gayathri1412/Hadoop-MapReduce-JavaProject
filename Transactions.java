/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datasets;

/**
 *
 * @author Gayathri
 */
import java.io.FileWriter;
import java.util.Random;

public class Transactions {

    private static void TransactionDataset() {
        try {
            long TransID;
            int CustID;
            float TransTotal;
            int TransNumItems;
            String TransDesc;
            String record;

            try (FileWriter FW = new FileWriter("Transactions.txt")) {
                for (int i = 1; i <= 5000000; i++) {
                    TransID = i;
                    CustID = new Random().nextInt(49999) + 1;
                    TransTotal = new Random().nextFloat() * 990 + 10;
                    TransNumItems = new Random().nextInt(9) + 1;
                    TransDesc = createTransDesc();

                    record = String.valueOf(TransID) + "," + String.valueOf(CustID) + "," + String.valueOf(TransTotal) + "," + String.valueOf(TransNumItems) + "," + TransDesc + "\r\n";
                    FW.write(record);
                }
            }
        } catch (Exception e) {

        }
    }

    public static String createTransDesc() {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= rand.nextInt(30) + 20; i++) {
            sb.append((char) (rand.nextInt(25) + 97));

        }
        return sb.toString();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        TransactionDataset();

    }

}
