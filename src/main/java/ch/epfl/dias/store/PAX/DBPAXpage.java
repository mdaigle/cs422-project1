package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.List;

public class DBPAXpage {
    private List<List<Object>> cols;
    private DataType[] schema;

    DBPAXpage(DataType[] schema) {
        this.schema = schema;
        this.cols = new ArrayList<>();
        for (DataType ignored : schema) {
            this.cols.add(new ArrayList<>());
        }
    }

    void addTuple(Object[] vals) {
        for (int i = 0; i < vals.length; i++) {
            cols.get(i).add(vals[i]);
        }
    }

    DBTuple getRow(int row_num) {
        if (row_num < 0 || row_num > cols.get(0).size()) {
            return new DBTuple();
        }

        Object[] fields = new Object[cols.size()];

        for (int i = 0; i < cols.size(); i++) {
            fields[i] = cols.get(i).get(row_num);
        }

        return new DBTuple(fields, schema);
    }
}
