package br.osprojects.mysql;

import br.osprojects.cassandra.OSDAO;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class MainMySQL {

    private String adress, port;
    protected static String KeySpace;
    protected static Connection connection;
    protected static final String Host = "localhost:9160", Adress = "localhost";
    //protected static final String KeySpace;// = "teste";
    //protected static Connection conn = new ConnectionFactory().getConnection();
    protected static OSDAO dao;
    protected static Cassandra.Client client;

    public MainMySQL(String adress, String port, String keyspace, String user, String pass) {
        this.adress = adress;
        this.port = port;
        this.KeySpace = keyspace;
        try {
            connection = DriverManager.getConnection("jdbc:mysql://" + adress + ":" + port + "/" + keyspace, user, pass);
        } catch (SQLException ex) {
        }
        try {
            init();
            Convert.processConvert();
        } catch (Exception ex) {
            System.out.println("Erro: "+ex.getMessage());
        }
    }
    
    public static void main(String[] args) throws Exception {
        //init();
        Convert.processConvert();


        System.out.println("Finalizado Conversão");

    }

    public void init() throws Exception {
        TSocket socket = new TSocket(Adress, 9160);
        socket.open();
        TTransport transport = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Cassandra.Client(protocol);
        client.set_keyspace(KeySpace);
    }

}