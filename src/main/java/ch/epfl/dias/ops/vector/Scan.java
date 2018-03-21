package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.List;

public class Scan implements VectorOperator {

	private Store store;
	private int vectorsize;
	private DBColumn[] cols;
	private int ctr;
	private boolean done;

	public Scan(Store store, int vectorsize) {
		this.store = store;
		this.vectorsize = vectorsize;
		this.ctr = 0;
	}
	
	@Override
	public void open() {
		store.load();
		cols = store.getColumns(null);
	}

	@Override
	public DBColumn[] next() {
		if (done) { return null; }

		List<DBColumn> next = new ArrayList<>();
		int end = ctr + vectorsize;

		for (DBColumn col : cols) {
			next.add(col.subset(ctr, end));
		}

		ctr += vectorsize;

		return (DBColumn[]) next.toArray();
	}

	@Override
	public void close() {
		// TODO: Implement
	}
}
