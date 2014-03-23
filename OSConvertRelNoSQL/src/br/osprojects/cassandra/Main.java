package br.osprojects.cassandra;

import java.util.HashMap;
import java.util.Map;

public class Main {

    private static int count;
    private static final String Host = "localhost:9160";
    private static final String KeySpace = "bibliotecaeeepjwm";
    private static OSDAO dao;

    public static void main(String[] args) throws Exception {
        //first create a object, connecting at the database, passing the 'Host' and 'Keyspace'
        dao = new OSDAO(Host, KeySpace);
        
                //use a Map String to set the columns values..
                 Map < String
        , String > user = new HashMap<String, String>();
        user.put("name", "Jos�");
         user.put("last", "Silva");
         user.put("cpf", "07179833401");
        count++;
        //write some values in the 'user' column family 
        dao.write("id_0001", user);

        //read a row, where the id value is 'id_0001'
        //dao.read("id_0001");

        //..and delete the row which its rowkey is 'id_0001'
        //dao.delete("id_0001");
    }
}
