package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ch.epfl.dias.store.DataType;

public class DBColumn {
	private DataType type;
	private List<Object> vals;

	public DBColumn(List<Object> vals, DataType type) {
		this.vals = vals;
		this.type = type;
	}

	public List<Object> getVals() {
		return Collections.unmodifiableList(vals);
	}

	public int size() { return vals.size(); }

	public DBColumn subset(List<Integer> indices) {
		List<Object> sub_vals = new ArrayList<>();

		for (int i = 0; i<indices.size(); i++) {
			sub_vals.add(vals.get(indices.get(i)));
		}

		return new DBColumn(sub_vals, type);
	}

	public DBColumn subset(int start, int end) {
		if (start >= size()) {
			return null;
		}

		List<Object> val_subset = new ArrayList<>();

		for (int i = start; i < end && i < size(); i++) {
			val_subset.add(vals.get(i));
		}

		return new DBColumn(val_subset, type);
	}

	public Double[] toDoubleArray() {
		// Get col as double array or fail
		Double[] vals = new Double[this.vals.size()];
		if (type == DataType.DOUBLE) {
			vals = getAsDouble();
		} else if (type == DataType.INT) {
			Integer[] ints = getAsInteger();
			for (int i = 0; i < vals.length; i++) {
				vals[i] = ints[i].doubleValue();
			}
		} else {
			return null;
		}

		return vals;
	}

	public DataType getType() {
		return type;
	}
	
	public Integer[] getAsInteger() {
		Integer[] out = new Integer[vals.size()];

		for (int i = 0; i < out.length; i++) {
			out[i] = (Integer) vals.get(i);
		}

		return out;
	}

	public Double[] getAsDouble() {
		Double[] out = new Double[vals.size()];

		for (int i = 0; i < out.length; i++) {
			out[i] = (Double) vals.get(i);
		}

		return out;
	}

	public Boolean[] getAsBoolean() {
		Boolean[] out = new Boolean[vals.size()];

		for (int i = 0; i < out.length; i++) {
			out[i] = (Boolean) vals.get(i);
		}

		return out;
	}

	public String[] getAsString() {
		String[] out = new String[vals.size()];

		for (int i = 0; i < out.length; i++) {
			out[i] = (String) vals.get(i);
		}

		return out;
	}
}
