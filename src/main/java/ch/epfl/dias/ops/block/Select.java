package ch.epfl.dias.ops.block;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements BlockOperator {

	private BlockOperator child;
	private BinaryOp op;
	private int fieldNo;
	private double value;

	public Select(BlockOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = (double) value;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] cols = child.execute();
		DBColumn select_col = cols[fieldNo];

		// Get col as double array or fail
		Double[] vals = select_col.toDoubleArray();

		// Find passing indices
		List<Integer> indices = new ArrayList<>();
		for (int i = 0; i < select_col.size(); i++) {
			switch (op) {
				case EQ:
					if (vals[i] == value) { indices.add(i); }
					break;
				case GE:
					if (vals[i] >= value) { indices.add(i); }
					break;
				case GT:
					if (vals[i] > value) { indices.add(i); }
					break;
				case LE:
					if (vals[i] <= value) { indices.add(i); }
					break;
				case LT:
					if (vals[i] < value) { indices.add(i); }
					break;
				case NE:
					if (vals[i] != value) { indices.add(i); }
			}
		}

		// Get subset of each column
		for (int i = 0; i < cols.length; i++) {
			cols[i] = cols[i].subset(indices);
		}

		return cols;
	}
}
