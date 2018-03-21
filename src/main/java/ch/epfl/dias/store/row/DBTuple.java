package ch.epfl.dias.store.row;

import ch.epfl.dias.store.DataType;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DBTuple {
	public Object[] fields;
	public DataType[] types;
	public boolean eof;

	public DBTuple(Object[] fields, DataType[] types) {
		this.fields = fields;
		this.types = types;
		this.eof = false;
	}

	public DBTuple() {
		this.eof = true;
	}

	public DBTuple join(DBTuple other) {
		List<Object> fields = new ArrayList<>();
		fields.addAll(Arrays.asList(this.fields));
		fields.addAll(Arrays.asList(other.fields));

		DataType[] types = new DataType[this.types.length + other.types.length];

		for (int i = 0; i < this.types.length; i++) {
			types[i] = this.types[i];
		}
		for (int j = 0; j < other.types.length; j++) {
			types[j + this.types.length] = other.types[j];
		}

		return new DBTuple(fields.toArray(), types);
	}

	public DBTuple subset(int[] fieldNos) {
		DataType[] types_subset = new DataType[fieldNos.length];
		Object[] fields_subset = new Object[fieldNos.length];

		for (int i = 0; i < fieldNos.length; i++) {
			types_subset[i] = types[fieldNos[i]];
			fields_subset[i] = fields[fieldNos[i]];
		}

		return new DBTuple(fields_subset, types_subset);
	}

	/**
	 * XXX Assuming that the caller has ALREADY checked the datatype, and has
	 * made the right call
	 * 
	 * @param fieldNo
	 *            (starting from 0)
	 * @return cast of field
	 */
	public Integer getFieldAsInt(int fieldNo) {
		return (Integer) fields[fieldNo];
	}

	public Double getFieldAsDouble(int fieldNo) {
		return (Double) fields[fieldNo];
	}

	public Boolean getFieldAsBoolean(int fieldNo) {
		return (Boolean) fields[fieldNo];
	}

	public String getFieldAsString(int fieldNo) {
		return (String) fields[fieldNo];
	}
}
