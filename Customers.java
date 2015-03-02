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

public class Customers {

    private static void CustomerDataset() {
        try {
            int ID;
            String Name;
            int Age;
            int CountryCode;
            float Salary;
            String record;

            try (FileWriter FW = new FileWriter("Customers.txt")) {
                for (int i = 1; i <= 50000; i++) {
                    ID = i;
                    Name = createName();
                    Age = new Random().nextInt(10) + 60;
                    CountryCode = new Random().nextInt(9) + 1;
                    Salary = new Random().nextFloat() * 9900 + 100;
                    
                    record = String.valueOf(ID) + "," + Name + "," + String.valueOf(Age) + "," + String.valueOf(CountryCode) + "," + String.valueOf(Salary) + "\r\n";
                    FW.write(record);
                }
            }
        } catch (Exception e) {

        }
    }

    public static String createName() {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= rand.nextInt(10) + 10; i++) {
            sb.append((char) (rand.nextInt(25) + 97));

        }
        return sb.toString();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        CustomerDataset();

    }

}
