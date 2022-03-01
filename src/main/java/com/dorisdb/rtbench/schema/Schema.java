package com.dorisdb.rtbench.schema;

import java.util.ArrayList;
import java.util.List;

import com.dorisdb.rtbench.DataOperation;
import com.dorisdb.rtbench.DataOperation.Op;
import com.dorisdb.rtbench.Utils;

public class Schema {
    public Column[] columns;
    public int nCol;
    public int nkey;
    public String[] columnNames;
    public int[] keyColumnIdxs;
    public int[] updatableIdxs;

    static int[] convertIntegers(List<Integer> integers)
    {
        int[] ret = new int[integers.size()];
        for (int i=0; i < ret.length; i++)
        {
            ret[i] = integers.get(i).intValue();
        }
        return ret;
    }

    public Schema(Column... cols) throws Exception {
        this.columns = cols;
        this.nCol = columns.length;
        int kmax = -1;
        columnNames = new String[columns.length];
        List<Integer> kl = new ArrayList<>();
        List<Integer> ul = new ArrayList<>();
        for (int i=0;i<columns.length;i++) {
            columnNames[i] = columns[i].name;
            if (columns[i].isKey) {
                nkey++;
                kmax = i;
                kl.add(i);
            }
            if (columns[i].updatable) {
                ul.add(i);
            }
        }
        if (kmax+1 != nkey) {
            throw new Exception("bad key spec in schema");
        }
        keyColumnIdxs = convertIntegers(kl);
        updatableIdxs = convertIntegers(ul);
    }

    public void genOp(long idx, long seed, long updateSeed, DataOperation op) {
        op.fullFieldNames = columnNames;
        op.keyFieldIdxs = keyColumnIdxs;
        op.fullFields = new Object[nCol];
        for (int i=0;i<nCol;i++) {
            op.fullFields[i] = columns[i].generate(idx, seed, updateSeed);
            seed = Utils.nextRand(seed);
        }
    }

    public void genOpNumerousColumns(long idx, long seed, long updateSeed, DataOperation op, int[] numerousPartialColumnIdxes) {
        op.fullFieldNames = columnNames;
        op.keyFieldIdxs = keyColumnIdxs;
        op.updateFieldIdxs = numerousPartialColumnIdxes;
        op.fullFields = new Object[numerousPartialColumnIdxes.length];
        for (int i=0;i<numerousPartialColumnIdxes.length;i++) {
            op.fullFields[i] = columns[numerousPartialColumnIdxes[i]].generate(idx, seed, updateSeed);
            seed = Utils.nextRand(seed);
        }
    }

    public String getCreateTableMySql(String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("create table %s (", tableName));
        for (int i=0;i<columns.length;i++) {
            Column c = columns[i];
            sb.append(String.format("%s%s %s%s", i==0 ? "":",", c.name, c.type, c.nullable ? " NULL" : " NOT NULL"));
        }
        sb.append(", primary key(");
        // primary key columns
        for (int i=0;i<nkey;i++) {
            if (i>0) {
                sb.append(",");
            }
            Column c = columns[i];
            sb.append(c.name);
        }
        sb.append("))");
        return sb.toString();
    }

    public String getCreateTableDorisDB(String tableName, int bucket, int replication) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("create table if not exists %s (", tableName));
        for (int i=0;i<columns.length;i++) {
            Column c = columns[i];
            sb.append(String.format("%s%s %s%s", i==0 ? "":",", c.name, c.type, c.nullable ? " NULL" : " NOT NULL"));
            sb.append(String.format(" default %s", c.defaultStr));
        }
        sb.append(") primary key(");
        // primary key columns
        for (int i=0;i<nkey;i++) {
            if (i>0) {
                sb.append(",");
            }
            Column c = columns[i];
            sb.append(c.name);
        }
        sb.append(") ");
        sb.append(String.format("DISTRIBUTED BY HASH(%s) BUCKETS %d" + " PROPERTIES(\"replication_num\" = \"%d\")",
                columns[nkey-1].name, bucket, replication));
        return sb.toString();
    }

}
