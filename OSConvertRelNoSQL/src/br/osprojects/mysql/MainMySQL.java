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

    protected static final String Host = "localhost:9160", Adress = "localhost";
    protected static final String KeySpace = "bibliotecaeeepjwm";
    protected static Connection conn = new ConnectionFactory().getConnection();
    protected static OSDAO dao;
    protected static Cassandra.Client client;

    public static void main(String[] args) throws Exception {
        init();
        Convert.processConvert();

//        try {
////            metaData = conn.getMetaData();
////            rsT = metaData.getPrimaryKeys(null, "osbiblio", "acervo");
////            System.out.println(rsT.getObject(0));
//            String[] tableTypes = {"TABLE_NAMES"};
//            //ResultSet rs1 = conn.getMetaData().getColumns(null, null, "", null);
//            //metaData = conn.getMetaData();
//            ResultSet rsSchemas = conn.getMetaData().getSchemas();
//            ResultSet rsTables;
//            
//            while(rsSchemas.next()){
//                rsTables = conn.getMetaData().getTables(null,rsSchemas.getString(1),"%",null);  
//                 while(rsTables.next()) {  
//                     System.out.println("Tables " + rsTables.getString(3));
//                 }
//            }
//            
//            rsT = conn.getMetaData().getTables(null, "osbiblio", "", null);
//
//            for (int i = 1; rsT.next(); i++) {
//                System.out.println(rs.getString(3));
//            }
////            while(rsT.next()){
////                System.out.println();
////            }
//            //String string = rsT.getString("TABLE_NAME");
//            // System.out.println(rsT.getString("TABLE_NAME"));
//        } catch (Exception e) {
//            System.out.println("Erro: " + e.getMessage());
//        }

//        DatabaseMetaData databaseMetaData;// = conn.getMetaData() ;
////Busca os Schemas do banco...  
//        ResultSet rsSchemas = databaseMetaData.getSchemas();
//        ResultSet rsTables, rsColumns;
//        while (rsSchemas.next()) {
//
//            //Traz todas as tabelas referente ao Schema corrente.  
//            rsTables = databaseMetaData.getTables(null, rsSchemas.getString(1), "%", null);
//            while (rsTables.next()) {
//
//                //Lista as tabelas...  
//                System.out.println("Tables " + rsTables.getString(3));
//
//                //Busca as colunas das tabelas...  
//                rsColumns = databaseMetaData.getColumns(null, rsSchemas.getString(1), rsTables.getString(3), null);
//
//                while (rsColumns.next()) {
//
//                    //Lista as colunas...  
//                    System.out.println("Columns " + rsColumns.getString(4));
//                }
//            }
//        }

        /*   ResultSet rs = st.executeQuery("shot tables");
         ResultSetMetaData rsMetaData = rs.getMetaData();


         int numberOfColumns = rsMetaData.getColumnCount();
         System.out.println("resultSet MetaData column Count=" + numberOfColumns);


         Object chavePrimaria = null;

         while (rs.next()) {
         for (int i = 1; i <= numberOfColumns; i++) {

         if (rsMetaData.isAutoIncrement(i)) {
         chavePrimaria = String.valueOf(rs.getObject(i));
         } else {
         if (rs.getObject(i) != null) {
         if (!String.valueOf(rs.getObject(i)).equals("")) {
         user.put(rsMetaData.getColumnName(i), String.valueOf(rs.getObject(i)));
         }
         }
         }
         */
//                System.out.println("column MetaData ");
//                System.out.println("column number " + i);
//                // indicates the designated column's normal maximum width in (largura máxima de uma coluna em caracteres)
//                // characters
//                System.out.println("tamanho da coluna em caracteres: " + rsMetaData.getColumnDisplaySize(i));
//                // gets the designated column's suggested title 
//                // for use in printouts and displays.
//                System.out.println(rsMetaData.getColumnLabel(i));
//                // get the designated column's name. (obtem o nome da coluna designada)
//                System.out.println("nome da coluna: " + rsMetaData.getColumnName(i));
//
//                // get the designated column's SQL type. (tipo da coluna designada)
//                System.out.println(rsMetaData.getColumnType(i));
//
//                // get the designated column's SQL type name. (nome do tipo sql da coluna designada)
//                System.out.println("tipo de dado: " + rsMetaData.getColumnTypeName(i));
//
//                // get the designated column's class name. (nome da classe da coluna)
//                System.out.println(rsMetaData.getColumnClassName(i));
//
//                // get the designated column's table name. (nome da coluna)
//                System.out.println(rsMetaData.getTableName(i));
//
//                // get the designated column's number of decimal digits. (numero de digitos decimais da coluna)
//                System.out.println(rsMetaData.getPrecision(i));
//
//                // gets the designated column's number of
//                // digits to right of the decimal point.
//                System.out.println(rsMetaData.getScale(i));
//
//                // indicates whether the designated column is
//                // automatically numbered, thus read-only. (indica se a coluna é auto incremento)
//                System.out.println("Se é auto incrmento: " + rsMetaData.isAutoIncrement(i));
//
//                // indicates whether the designated column is a cash value. (indica se é um valor em dinheiro)
//                System.out.println(rsMetaData.isCurrency(i));
//
//                // indicates whether a write on the designated
//                // column will succeed. (se pode ser escrito algo na coluna)
//                System.out.println(rsMetaData.isWritable(i));
//
//                // indicates whether a write on the designated
//                // column will definitely succeed.
//                System.out.println(rsMetaData.isDefinitelyWritable(i));
//
//                // indicates the nullability of values
//                // in the designated column. (se vai pode ser not null)
//                System.out.println("S epode ser nula: " + rsMetaData.isNullable(i));
//
//                // Indicates whether the designated column
//                // is definitely not writable.
//                System.out.println(rsMetaData.isReadOnly(i));
//
//                // Indicates whether a column's case matters
//                // in the designated column.
//                System.out.println(rsMetaData.isCaseSensitive(i));
//
//                // Indicates whether a column's case matters
//                // in the designated column.
//                System.out.println(rsMetaData.isSearchable(i));
//
//                // indicates whether values in the designated
//                // column are signed numbers.
//                System.out.println(rsMetaData.isSigned(i));
//
//                // Gets the designated column's table's catalog name. (nome do catalógo da coluna)
//                System.out.println("Nome do catalogo: " + rsMetaData.getCatalogName(i));
//
//                // Gets the designated column's table's schema name. (obtem o nome do esquema)
//                System.out.println("Nme esquema: " + rsMetaData.getSchemaName(i));
        // }
        //    dao.write(String.valueOf(chavePrimaria), user);
        //   user = new HashMap<String, String>();

        //  }
        System.out.println("Finalizado Conversão");
        //st.close();
        //conn.close();
    }

    private static Connection getHSQLConnection() throws Exception {
        Class.forName("org.hsqldb.jdbcDriver");
        System.out.println("Driver Loaded.");
        String url = "jdbc:hsqldb:data/tutorial";
        return DriverManager.getConnection(url, "sa", "");
    }

    public static Connection getMySqlConnection() throws Exception {
        String driver = "org.gjt.mm.mysql.Driver";
        String url = "jdbc:mysql://localhost//" + KeySpace;
        String username = "root";
        String password = "";

        Class.forName(driver);
        conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

    public static Connection getOracleConnection() throws Exception {
        String driver = "oracle.jdbc.driver.OracleDriver";
        String url = "jdbc:oracle:thin:@localhost:1521:caspian";
        String username = "mp";
        String password = "mp2";

        Class.forName(driver); // load Oracle driver
        conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

    public static void init() throws Exception {
        TSocket socket = new TSocket(Adress, 9160);
        socket.open();
        TTransport transport = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Cassandra.Client(protocol);
        client.set_keyspace(KeySpace);
    }

/*    public static void processConvert() throws Exception {
        Statement sts;
        DatabaseMetaData metaData;
        ResultSet resultSet;
        ResultSetMetaData importKeyR;
        CfDef columnFamily;
        ByteBuffer bytes;
        ColumnParent parent;
        StringBuilder key_id = new StringBuilder("1");
        List<ResultSet> estrangeiras = new ArrayList<>(), listaResultados = new ArrayList<>(), listaResultados2 = new ArrayList<>();
        try {
            resultSet = dbmd.getTables(null, null, "%", null);
            while (resultSet.next()) {
                if (columnsFamily.contains(resultSet.getString(3))) {
                    continue;
                }
                columnFamilyCreated = false;
                String tableName = resultSet.getString(3); /* Nome da Tabela */
       /*         System.out.println(tableName.toString());
                columnFamily = new CfDef();
                columnFamily.setKeyspace(KeySpace);
                columnFamily.setName(tableName);
                ResultSet rForeignKey = dbmd.getImportedKeys(null, null, tableName);
                ResultSet chavesEstrangeiras = dbmd.getExportedKeys(null, null, tableName);
                /* Passa todas as colunas encontradas na tabela para o método convertSampleColumns */
        /*        convertSampleColumns(columnFamily, dbmd.getColumns(null, null, tableName, null), dbmd.getPrimaryKeys(null, null, tableName), rForeignKey, chavesEstrangeiras);
                columnsFamily.add(resultSet.getString(3));
                columnFamilyCreated = true;

                /* Converte as chaves importadas
                 ResultSet chavesImportadas = dbmd.getImportedKeys(null, null, tableName);
                 convertImportColumns(columnFamily, chavesImportadas);
                 columnsFamily.add(resultSet.getString(3));*/

                /* Converte as chaves estrangeiras
                 * ResultSet chavesEstrangeiras = dbmd.getImportedKeys(null, null, tableName);
                 convertExportColumns(columnFamily, chavesEstrangeiras);*/

                /* ResultSet contendo o relacionamento interno de uma tabela (importKeys). */
                /*      ResultSet rForeignKey = dbmd.getImportedKeys(null, null, tableName);
                 /*
                 estrangeiras.add(dbmd.getExportedKeys(null, null, tableName));
                 while (rForeignKey.next()) {
                 String importTableName = rForeignKey.getString("PKTABLE_NAME");
                 ResultSet importKey = dbmd.getColumns(null, null, importTableName, null);
                 listaResultados.add(importKey);
                 for (int l = 1; importKey.next(); l++) {
                 ResultSet rPrimary = dbmd.getPrimaryKeys(null, null, importKey.getString(3));
                 rPrimary.first();
                 listaResultados2.add(rPrimary);
                 */
                /* Verifica se a coluna é diferente de uma chave primária.*/
                /*         if (!listaResultados.contains(listaResultados2.get(l - 1)) && l > 1) {
                 columnFamily.setKeyspace(KeySpace);
                 columnFamily.setName(tableName);
                 ColumnDef columnDef = new ColumnDef();
                 columnDef.setName(importTableName.getBytes());
                 System.out.println("" + importKey.getString("TYPE_NAME"));
                 String Type = getColumnType(importKey.getString("TYPE_NAME"));
                 columnFamily.setDefault_validation_class(Type);
                 columnFamily.setKey_validation_class(Type);
                 columnDef.setValidation_class(Type);
                 Column column = new Column();
                 column.setName(importKey.getString("COLUMN_NAME").getBytes());
                 columnFamily.addToColumn_metadata(columnDef);
                 column.setTimestamp(System.currentTimeMillis());
                 if (!columnFamilyCreated) {
                 client.system_add_column_family(columnFamily);
                 columnFamilyCreated = true;
                 } else {
                 client.system_update_column_family(columnFamily);
                 }
                 parent = new ColumnParent(columnFamily.getName());
                 if (importKey.getMetaData().getColumnCount() > 0 && !estrangeiras.contains(listaResultados2.get(l - 1))) {
                 sts = conn.createStatement();
                 System.out.println("SELECT " + importKey.getString("COLUMN_NAME") + " FROM " + importTableName + "ggh".getBytes());
                 ResultSet values = sts.executeQuery("SELECT " + importKey.getString("COLUMN_NAME") + " FROM " + importTableName);
                 for (int y = 1; values.next(); y++) {
                 if (y > 1) {
                 column = new Column();
                 column.setName(importKey.getString("COLUMN_NAME").getBytes());
                 column.setTimestamp(System.currentTimeMillis());
                 }
                 column.setValue(ByteBuffer.wrap(values.getString(1).getBytes()));
                 client.insert(ByteBuffer.wrap(key_id.toString().getBytes()), parent, column, ConsistencyLevel.ALL);
                 key_id.replace(0, key_id.length(), String.valueOf(Integer.parseInt(key_id.toString()) + 1));
                 }
                 values.close();
                 sts.close();
                 } else {
                 client.insert(ByteBuffer.wrap(key_id.toString().getBytes()), parent, column, ConsistencyLevel.ALL);
                 key_id.replace(0, key_id.length() - 1, String.valueOf(Integer.parseInt(key_id.toString()) + 1));
                 }
                 }
                 }
                 }*/
            }
       /* } catch (SQLException e) {
            JOptionPane.showMessageDialog(null, "Ocorreu um erro na conversão: " + e.getMessage());
        }
    }*/
/*
    public static String getColumnType(String name) {
        switch (name) {
            case "bigint":
                return "LongType";
            case "blob":
                return "BytesType";
            case "boolean":
                return "BooleanType";
            case "decimal":
                return "DecimalType";
            case "float":
                return "FloatType";
            case "int":
            case "smallint":
            case "tinyint":
                return "IntegerType";
            default:
                return "UTF8Type";
        }
    }*/

   /* public static void convertSampleColumns(CfDef columnFamily, ResultSet colunas, ResultSet chavesPrimarias,
            ResultSet chavesImportadas, ResultSet chavesEstrangeiras) throws Exception {

        List<String> listaChavesPrimarias = new ArrayList<>(), listaChavesImportadas = new ArrayList<>(),
                listaChavesEstrangeiras = new ArrayList<>();
        while (chavesPrimarias.next()) {
            listaChavesPrimarias.add(chavesPrimarias.getString(4).toLowerCase());
        }
        while (chavesImportadas.next()) {
            listaChavesImportadas.add(chavesImportadas.getString(4).toLowerCase());
        }
        while (chavesEstrangeiras.next()) {
            listaChavesEstrangeiras.add(chavesEstrangeiras.getString(4).toLowerCase());
        }
        while (colunas.next()) {
            if (!listaChavesPrimarias.contains(colunas.getString(4))
                    && !listaChavesImportadas.contains(colunas.getString(4))
                    && !listaChavesEstrangeiras.contains(colunas.getString(4))) {
                columnsDefinitions(colunas, columnFamily, listaChavesImportadas, listaChavesEstrangeiras);
            }
        }
    }*/

/*    public static void convertImportColumns(CfDef columnFamily, ResultSet importKeys) throws Exception {
        StringBuilder key_id = new StringBuilder("1");
        ColumnDef columnDef;
        while (importKeys.next()) {
            System.out.println(importKeys.getString(4));
            ResultSet importKey = dbmd.getColumns(null, null, importKeys.getString(3), null);
            importKey.next();
            while (importKey.next()) {
                System.out.println("" + importKey.getString(6));
                String Type = getColumnType(importKey.getString(6));
                columnFamily.setDefault_validation_class(Type);
                columnFamily.setKey_validation_class(Type);
                columnDef = new ColumnDef();
                columnDef.setValidation_class(Type);
                columnDef.setName(importKeys.getString(3).getBytes());
                Column column = new Column();
                column.setName(importKey.getString(4).getBytes());
                columnFamily.addToColumn_metadata(columnDef);
                column.setTimestamp(System.currentTimeMillis());
                ColumnParent parent = new ColumnParent(columnFamily.getName());
                if (!columnFamilyCreated && !columnsFamily.contains(columnFamily.name)) {
                    client.system_add_column_family(columnFamily);
                    columnFamilyCreated = true;
                } else {
                    client.system_update_column_family(columnFamily);
                }
                if (importKey.getMetaData().getColumnCount() > 0) {
                    Statement st = conn.createStatement();
                    System.out.println("SELECT " + importKey.getString("COLUMN_NAME") + " FROM " + importKeys.getString(3) + "ggh".getBytes());
                    ResultSet values = st.executeQuery("SELECT " + importKey.getString("COLUMN_NAME") + " FROM " + importKeys.getString(3));
                    for (int y = 1; values.next(); y++) {
                        if (values.getString(1) == null) {
                            continue;

                        }
                        if (y > 1) {
                            column = new Column();
                            column.setName(importKey.getString("COLUMN_NAME").getBytes());
                            column.setTimestamp(System.currentTimeMillis());
                        }
                        column.setValue(ByteBuffer.wrap(values.getString(1).getBytes()));
                        try {
                            client.insert(ByteBuffer.wrap(key_id.toString().getBytes()), parent, column, ConsistencyLevel.ALL);
                        } catch (InvalidRequestException | UnavailableException | TimedOutException | TException ex) {
                            JOptionPane.showMessageDialog(null, ex.getMessage());
                        }
                        key_id.replace(0, key_id.length(), String.valueOf(Integer.parseInt(key_id.toString()) + 1));
                    }
                    values.close();
                    st.close();
                }
            }
        }
    }

/*    public static void convertExportColumns(CfDef columnFamily, ResultSet exportKeys) throws Exception {
        StringBuilder key_id = new StringBuilder("1");
        List<String> listaChavesPrimarias = new ArrayList<>(), listafields = new ArrayList<>();
        ColumnDef columnDef;
        exportKeys.next();
        while (exportKeys.next()) {
            if (listafields.contains(exportKeys.getString(7))) {
                continue;
            }
            listafields.add(exportKeys.getString(7));
            ResultSet importKey = dbmd.getColumns(null, null, exportKeys.getString(7), null);
            importKey.next();
            externLoop:
            while (importKey.next()) {
                System.out.println(importKey.getString(6));
                String Type = getColumnType(importKey.getString(6));
                columnFamily.setDefault_validation_class("UTF8Type");
                columnFamily.setKey_validation_class("UTF8Type");
                columnFamily.setComparator_type("UTF8Type");
                columnDef = new ColumnDef();
                columnDef.setName(exportKeys.getString(7).getBytes());
                columnDef.setValidation_class(Type);
                ColumnParent parent = new ColumnParent(columnFamily.getName());
                columnFamily.addToColumn_metadata(columnDef);
                if (!columnFamilyCreated && !exportKeys.getString(3).equals(exportKeys.getString(7))) {
                    client.system_add_column_family(columnFamily);
                    columnFamilyCreated = true;
                } else {
                    client.system_update_column_family(columnFamily);
                }
                Column column = new Column();
                if (importKey.getMetaData().getColumnCount() > 0) {
                    Statement st = conn.createStatement();
                    System.out.println(exportKeys.getString(7));
                    ResultSet getExportedColumns = dbmd.getImportedKeys(null, null, exportKeys.getString(7));
                    while (getExportedColumns.next()) {
                        /* Coleta as chaves primárias da coluna estrangeira */
  /*                      ResultSet primarias = dbmd.getPrimaryKeys(null, null, getExportedColumns.getString(3));
                        while (primarias.next()) {
                            listaChavesPrimarias.add(primarias.getString(4));
                        }
                        ResultSet getExportedColumnsExported = dbmd.getColumns(null, null, getExportedColumns.getString(3), null);
                        while (getExportedColumnsExported.next()) {
                            if (!listaChavesPrimarias.contains(getExportedColumnsExported.getString("COLUMN_NAME"))) {
                                System.out.println("SELECT " + getExportedColumnsExported.getString("COLUMN_NAME") + " FROM " + getExportedColumns.getString(3) + "ggh".getBytes());
                                ResultSet values = st.executeQuery("SELECT " + getExportedColumnsExported.getString("COLUMN_NAME") + " FROM " + getExportedColumns.getString(3));
                                for (int y = 1; values.next(); y++) {
                                    if (values.getString(1) == null) {
                                        continue;
                                    } else if (y > 1) {
                                        column = new Column();
                                    }
                                    column.setName(getExportedColumnsExported.getString(4).getBytes());
                                    column.setValue(ByteBuffer.wrap(values.getString(1).getBytes()));
                                    column.setTimestamp(System.currentTimeMillis());
                                    try {
                                        client.insert(ByteBuffer.wrap(key_id.toString().getBytes()), parent, column, ConsistencyLevel.ALL);
                                    } catch (InvalidRequestException | UnavailableException | TimedOutException | TException ex) {
                                        JOptionPane.showMessageDialog(null, ex.getMessage());
                                    }
                                    key_id.replace(0, key_id.length(), String.valueOf(Integer.parseInt(key_id.toString()) + 1));
                                }
                                values.close();
                            }
                        }
                        break externLoop;
                    }
                    st.close();
                }
            }
        }
    }*/

   /* public static void columnsDefinitions(ResultSet colunas, CfDef columnFamily, List<String> chavesImportadas,
            List<String> chavesEstrangeiras) throws Exception {
        StringBuilder key_id = new StringBuilder("1");
        ColumnDef columnDef;
        System.out.println(colunas.getString(4));
        while (colunas.next()) {
            if (chavesImportadas.contains(colunas.getString(4))
                    || chavesEstrangeiras.contains(colunas.getString(4))) {
                continue;
            }
            System.out.println("" + colunas.getString(6));
            String Type = getColumnType(colunas.getString(6));
            columnFamily.setDefault_validation_class(Type);
            columnFamily.setKey_validation_class(Type);
            columnFamily.setComparator_type("UTF8Type");
            Column column = new Column();
            column.setName(colunas.getString(4).getBytes());
            columnDef = new ColumnDef();
            columnDef.setName(colunas.getString(4).getBytes());
            columnDef.setValidation_class("UTF8Type");
            columnFamily.addToColumn_metadata(columnDef);
            column.setTimestamp(System.currentTimeMillis());
            ColumnParent parent = new ColumnParent(columnFamily.getName());
            if (!columnFamilyCreated) {
                client.system_add_column_family(columnFamily);
                columnFamilyCreated = true;
            } else {
                client.system_update_column_family(columnFamily);
            }
            if (colunas.getMetaData().getColumnCount() > 0) {
                Statement st = conn.createStatement();
                System.out.println("SELECT " + colunas.getString("COLUMN_NAME") + " FROM " + colunas.getString(3) + "ggh".getBytes());
                ResultSet values = st.executeQuery("SELECT " + colunas.getString("COLUMN_NAME") + " FROM " + colunas.getString(3));
                for (int y = 1; values.next(); y++) {
                    if (values.getString(1) == null) {
                        continue;
                    }
                    if (y > 1) {
                        column = new Column();
                        column.setName(colunas.getString("COLUMN_NAME").getBytes());
                        column.setTimestamp(System.currentTimeMillis());
                    }
                    column.setValue(ByteBuffer.wrap(values.getString(1).getBytes()));
                    try {
                        client.insert(ByteBuffer.wrap(key_id.toString().getBytes()), parent, column, ConsistencyLevel.ALL);
                    } catch (InvalidRequestException | UnavailableException | TimedOutException | TException ex) {
                        JOptionPane.showMessageDialog(null, ex.getMessage());
                    }
                    key_id.replace(0, key_id.length(), String.valueOf(Integer.parseInt(key_id.toString()) + 1));
                }
                values.close();
                st.close();
            }
        }
}
}
}*/