package br.osprojects.cassandra;

import java.util.Map;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

public class OSDAO extends AstyanaxDao {
	
    public static String nomeColumnFamily = "Acervo";
    
    private static final ColumnFamily<String, String> COLUMN_FAMILY = new ColumnFamily<>(nomeColumnFamily,
            StringSerializer.get(), StringSerializer.get());

    public OSDAO(String host, String keyspace) {
        super(host, keyspace);
        //nomeColumnFamily = columnFamily;
    }

    public void write(String rowKey, Map<String, String> columns) {
        MutationBatch mutation = this.getKeyspace().prepareMutationBatch();
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            mutation.withRow(COLUMN_FAMILY, rowKey).
            	putColumn(entry.getKey(), entry.getValue(), null);
        }
        try {
			mutation.execute();
			System.out.println("Wrote user [" + rowKey + "]");

        } catch (ConnectionException e) {
			e.printStackTrace();
		}
    }

    public ColumnList<String> read(String rowKey) throws ConnectionException {
        OperationResult<ColumnList<String>> result = this.getKeyspace().prepareQuery(COLUMN_FAMILY).getKey(rowKey)
                .execute();
        ColumnList<String> child = result.getResult();
        System.out.println("Read user [" + rowKey + "]");
        return child;
    }

    public void delete(String rowKey) {
        MutationBatch mutation = this.getKeyspace().prepareMutationBatch();
        mutation.withRow(COLUMN_FAMILY, rowKey).delete();
        try {
			mutation.execute();
			System.out.println("Deleted user [" + rowKey + "]");
		} catch (ConnectionException e) {
			
			e.printStackTrace();
		}
        

    }
}
