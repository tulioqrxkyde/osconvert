/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.osprojects.mysql;

import static br.osprojects.mysql.MainMySQL.client;
import static br.osprojects.mysql.MainMySQL.conn;
import java.nio.ByteBuffer;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.swing.JOptionPane;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

/**
 *
 * @author tulio.xcrtf
 */
public class Convert extends MainMySQL {

    private static DatabaseMetaData dbmd;
    private static Statement st;
    private static CfDef columnFamily;
    private static ColumnParent parent;
    private static ColumnDef columnDef;
    private static Column column;
    private static StringBuilder key_id, firstColumn;
    private static List<String> listaTabelas = new ArrayList<>(), listaColunas = new ArrayList<>();
    private static boolean columnFamilyCreated;
    private List<String> listaChavesPrimarias, listaChavesImportadas,
            listaChavesEstrangeiras;

    /**
     * Método que inicializa o processo de conversão.
     *
     * @throws Exception
     */
    public static void processConvert() throws Exception {
        dbmd = conn.getMetaData();
        try {
            ResultSet tabelas = dbmd.getTables(null, null, "%", null);
            while (tabelas.next()) {
                if (listaTabelas.contains(tabelas.getString(3))) {
                    continue;
                }
                columnFamilyCreated = false;
                String tableName = tabelas.getString(3); /* Nome da Tabela */
                columnFamily = new CfDef();
                columnFamily.setKeyspace(KeySpace);
                columnFamily.setName(tableName);
                ResultSet colunas = dbmd.getColumns(null, null, tableName, null);
                ResultSet chavesPrimarias = dbmd.getPrimaryKeys(null, null, tableName);
                ResultSet chavesImportadas = dbmd.getImportedKeys(null, null, tableName);
                ResultSet chavesEstrangeiras = dbmd.getExportedKeys(null, null, tableName);
                Convert c = new Convert();
                c.listIncrements(chavesPrimarias, chavesImportadas, chavesEstrangeiras);

                /* Passa todas as colunas encontradas na tabela para o método convertSampleColumns */
                // c.convertSampleColumns(columnFamily, colunas);

                // listaTabelas.add(tableName);
                // c.listIncrements(chavesPrimarias, chavesImportadas, chavesEstrangeiras);

                c.convertImportColumns(columnFamily, dbmd.getImportedKeys(null, null, tableName));
                listaTabelas.add(tableName);
            }
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(null, "Ocorreu um erro na conversão: " + e.getMessage());
        }
    }

    /**
     * Método que preenche todas as listas[Chaves Primárias, Chaves Importadas e
     * Chaves Estrangeiras].
     *
     * @param rsPrimarias ResultSet.
     * @param rsImportadas ResultSet.
     * @param rsEstrangeiras ResultSet.
     * @throws SQLException
     */
    public void listIncrements(ResultSet rsPrimarias,
            ResultSet rsImportadas, ResultSet rsEstrangeiras) throws SQLException {

        listaChavesPrimarias = new ArrayList<>();
        listaChavesImportadas = new ArrayList<>();
        listaChavesEstrangeiras = new ArrayList<>();
        while (rsPrimarias.next()) {
            listaChavesPrimarias.add(rsPrimarias.getString(4).toLowerCase());
        }
        while (rsImportadas.next()) {
            listaChavesImportadas.add(rsImportadas.getString(4).toLowerCase());
        }
        while (rsEstrangeiras.next()) {
            listaChavesEstrangeiras.add(rsEstrangeiras.getString(4).toLowerCase());
        }
    }

    /**
     * Método de conversão das Colunas normais.
     *
     * @param columnFamily ResultSet.
     * @param colunas ResultSet.
     * @throws Exception ResultSet.
     */
    public void convertSampleColumns(CfDef columnFamily, ResultSet colunas) throws Exception {

        while (colunas.next()) {
            if (!listaChavesImportadas.contains(colunas.getString(4))
                    && !listaChavesEstrangeiras.contains(colunas.getString(4))) {
                columnsDefinitions(new ResultSet[]{colunas}, 1);
                getColumnsValues(new ResultSet[]{colunas}, 1);
            } else {
                continue;
            }
            columnFamilyCreated = true;
        }
        firstColumn = null;
    }

    /**
     * Método de conversão das Colunas Importadas(importKeys).
     *
     * @param columnFamily CfDef.
     * @param importKeys ResultSet.
     * @throws Exception
     */
    public void convertImportColumns(CfDef columnFamily, ResultSet importKeys) throws Exception {

        while (importKeys.next()) {
            ResultSet colunas = dbmd.getColumns(null, null, importKeys.getString(3), null);
            System.out.println(importKeys.getString(3));
            while (colunas.next()) {
                System.out.println(importKeys.getString(4).toLowerCase());
                System.out.println(colunas.getString(4));
                //if (listaChavesImportadas.contains(importKeys.getString(4).toLowerCase())) {
                if (colunas.getString(6) == null) {
                    continue;
                }
                columnsDefinitions(new ResultSet[]{colunas}, 2);
                getColumnsValues(new ResultSet[]{colunas}, 2);
                //} else {
                //    continue;
                //}
            }
            columnFamilyCreated = true;
            firstColumn = null; /* Reseta o armazenamento da 1º coluna de uma tabela */
        }

    }

    /**
     * Método de conversão das Colunas Estrangeiras(exportKeys).
     *
     * @param columnFamily CfDef.
     * @param exportKeys ResultSet.
     * @throws Exception
     */
    public void convertExportColumns(CfDef columnFamily, ResultSet exportKeys) throws Exception {
        List<String> listafields = new ArrayList<>();
        exportKeys.next();
        while (exportKeys.next()) {
            if (listaChavesPrimarias.contains(exportKeys.getString(7))
                    || listaChavesImportadas.contains(exportKeys.getString(7))
                    || listaChavesEstrangeiras.contains(exportKeys.getString(7))
                    || listafields.contains(exportKeys.getString(7))) {
                continue;
            }
            listafields.add(exportKeys.getString(7));
            ResultSet importKey = dbmd.getColumns(null, null, exportKeys.getString(7), null);
            importKey.next();
            externLoop:
            while (importKey.next()) {
                columnsDefinitions(new ResultSet[]{importKey, exportKeys}, 3);
                getColumnsValues(new ResultSet[]{importKey, exportKeys}, 3);
            }
            columnFamilyCreated = true;
        }
        firstColumn = null;
    }

    /**
     * Método de definição e criação das Columns Families, ColumnsMetaData e
     * Columns.
     *
     * @param colunas ResultSet[].
     * @param type int.
     * @throws Exception
     */
    public void columnsDefinitions(ResultSet[] colunas, int type) throws Exception {
        key_id = new StringBuilder("1");
        if (type == 2) {
            /*getImportedKeys = dbmd.getImportedKeys(null, null, colunas[0].getString(3));
            getPrimaryKeys = dbmd.getPrimaryKeys(null, null, colunas[0].getString(3));
            getExportedKeys = dbmd.getExportedKeys(null, null, colunas[0].getString(3));
            listIncrements(getPrimaryKeys, getImportedKeys, getExportedKeys);*/
            if (listaChavesPrimarias.contains(colunas[0].getString(3).toLowerCase())) {
                return;
            }
        }
        String Type = getColumnType(type != 3 ? colunas[0].getString(6) : colunas[0].getString(7));
        columnFamily.setDefault_validation_class("UTF8Type");
        columnFamily.setKey_validation_class("UTF8Type");
        columnFamily.setComparator_type("UTF8Type");
        columnDef = new ColumnDef();
        columnDef.setValidation_class(Type);
        if (type == 1) {
            columnDef.setName(((listaColunas.contains(colunas[0].getString(4))) ? colunas[0].getString(3).concat(colunas[0].getString(4)).getBytes() : colunas[0].getString(4).getBytes()));
            listaColunas.add(colunas[0].getString(4));
        } else if (type == 2) {
            columnDef.setName(colunas[0].getString(3).getBytes());
            listaColunas.add(colunas[0].getString(4));
        } else {
            columnDef.setName(listaColunas.contains(colunas[1].getString(4)) ? colunas[1].getString(3).concat(colunas[1].getString(4)).getBytes() : colunas[1].getString(4).getBytes());
            listaColunas.add(colunas[0].getString(4));
        }
        column = new Column();
        column.setName(colunas[0].getString(4).getBytes());
        column.setTimestamp(System.currentTimeMillis());
        parent = new ColumnParent(columnFamily.getName());
        columnFamily.addToColumn_metadata(columnDef);
        if ((!(listaTabelas.contains(columnFamily.name))) && !columnFamilyCreated) {
            client.system_add_column_family(columnFamily);
        } else {
            client.system_update_column_family(columnFamily);
        }
    }

    /**
     * Método de obtenção dos valores das colunas. É efetuada uma query que será
     * transportada para a column family, dentro de sua coluna respectiva.
     *
     * @param colunas ResultSet[] colunas.
     * @param type VarArg int.
     * @throws Exception
     */
    public void getColumnsValues(ResultSet[] colunas, int type) throws Exception {
        ResultSet values = null, getExportedColumnsExported = null,
                valuesPKs = dbmd.getPrimaryKeys(null, null, colunas[0].getString(3));
        if (colunas[0].getMetaData().getColumnCount() > 0) {

            if (firstColumn == null) {
                firstColumn = new StringBuilder(colunas[0].getString("COLUMN_NAME"));
            }

            st = conn.createStatement();
            if (type == 3) {
                ResultSet getExportedColumns = dbmd.getImportedKeys(null, null, colunas[1].getString(7));
                while (getExportedColumns.next()) {
                    getExportedColumnsExported = dbmd.getColumns(null, null, getExportedColumns.getString(3), null);
                    while (getExportedColumnsExported.next()) {
                        if (!listaChavesPrimarias.contains(getExportedColumnsExported.getString("COLUMN_NAME"))) {
                            if (listaChavesPrimarias.isEmpty()) {
                                System.out.println("SELECT " + getExportedColumnsExported.getString("COLUMN_NAME") + " FROM " + getExportedColumns.getString(3) + " ORDER BY " + firstColumn.toString());
                                values = st.executeQuery("SELECT " + getExportedColumnsExported.getString("COLUMN_NAME") + " FROM " + getExportedColumns.getString(3) + " ORDER BY " + firstColumn.toString());
                            } else {
                                System.out.println("SELECT " + getExportedColumnsExported.getString("COLUMN_NAME") + " FROM " + getExportedColumns.getString(3) + " ORDER BY " + listaChavesPrimarias.get(0));
                                values = st.executeQuery("SELECT " + getExportedColumnsExported.getString("COLUMN_NAME") + " FROM " + getExportedColumns.getString(3) + " ORDER BY " + listaChavesPrimarias.get(0));
                            }
                        }
                    }
                }
            } else {
                ResultSet getImportedKeys, getPrimaryKeys, getExportedKeys;
                if (type == 2) {
                    getImportedKeys = dbmd.getImportedKeys(null, null, colunas[0].getString(3));
                    getPrimaryKeys = dbmd.getPrimaryKeys(null, null, colunas[0].getString(3));
                    getExportedKeys = dbmd.getExportedKeys(null, null, colunas[0].getString(3));
                    listIncrements(getPrimaryKeys, getImportedKeys, getExportedKeys);
                    if (listaChavesPrimarias.contains(colunas[0].getString(3).toLowerCase())) {
                        return;
                    }
                }

                if (listaChavesPrimarias.isEmpty() || type == 2) {
                    System.out.println("SELECT " + colunas[0].getString("COLUMN_NAME") + " FROM " + colunas[0].getString(3) + " ORDER BY " + firstColumn.toString());
                    values = st.executeQuery("SELECT " + colunas[0].getString("COLUMN_NAME") + " FROM " + colunas[0].getString(3) + " ORDER BY " + firstColumn.toString());
                } else if (type == 1) {
                    System.out.println("SELECT " + colunas[0].getString("COLUMN_NAME") + " FROM " + colunas[0].getString(3) + " ORDER BY " + listaChavesPrimarias.get(0));
                    values = st.executeQuery("SELECT " + colunas[0].getString("COLUMN_NAME") + " FROM " + colunas[0].getString(3) + " ORDER BY " + listaChavesPrimarias.get(0));
                }
            }
            for (int y = 1; values != null && values.next(); y++) {
                if (y > 1) {
                    column = new Column();
                    column.setName(getExportedColumnsExported == null ? colunas[0].getString("COLUMN_NAME").getBytes()
                            : getExportedColumnsExported.getString(4).getBytes());
                    column.setTimestamp(System.currentTimeMillis());
                }
                if (values.getString(1) != null && !(values.getString(1).isEmpty())) {
                    column.setValue(ByteBuffer.wrap(values.getString(1).getBytes()));
                }
                try {
                    if (column.value != null) {
                        client.insert(ByteBuffer.wrap(key_id.toString().getBytes()), parent, column, ConsistencyLevel.ALL);
                    }
                    client.system_update_column_family(columnFamily);
                } catch (InvalidRequestException | UnavailableException | TimedOutException | TException ex) {
                    JOptionPane.showMessageDialog(null, ex.getMessage());
                }
                key_id.replace(0, key_id.length(), String.valueOf(Integer.parseInt(key_id.toString()) + 1));
                columnFamilyCreated = true;
            }
        }
        values.close();
        st.close();
    }

    /**
     * Método de obtenção do tipo da coluna no modelo Relacional e o transforma
     * para o modelo NOSql(Cassandra).
     *
     * @param name String.
     * @return String type.
     */
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
            default:
                return "UTF8Type";
        }
    }
}