package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	private Store store;
	private int ctr;

	public Scan(Store store) {
		this.store = store;
		ctr = 0;
	}

	@Override
	public void open() {
		store.load();
	}

	@Override
	public DBTuple next() {
		return store.getRow(ctr++);
	}

	@Override
	public void close() {
	}
}