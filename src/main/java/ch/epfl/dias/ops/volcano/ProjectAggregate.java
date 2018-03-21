package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ProjectAggregate implements VolcanoOperator {

	private VolcanoOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	private boolean done = false;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBTuple next() {
		if (done) { return new DBTuple(); }

		Object val;
		switch (agg) {
			case AVG:
				val = average();
				break;
			case MAX:
				val = max();
				break;
			case MIN:
				val = min();
				break;
			case SUM:
				val = sum();
				break;
			case COUNT:
				val = count();
				break;
			default:
				return new DBTuple();
		}

		done = true;

		return new DBTuple(new Object[]{val}, new DataType[]{dt});
	}

	@Override
	public void close() {
		child.close();
	}

	private Object average() {
		double sum = 0;
		int count = 0;

		DBTuple next = child.next();
		while (!next.eof) {
			sum += next.getFieldAsDouble(fieldNo);
			count++;
		}

		return sum / count;
	}

	private Object max() {
		double max = 0;

		DBTuple next = child.next();
		while (!next.eof) {
			if (next.getFieldAsDouble(fieldNo) > max) {
				max = next.getFieldAsDouble(fieldNo);
			}
		}

		if (dt == DataType.INT) {
			return (int) Math.round(max);
		}

		return max;
	}

	private Object min() {
		double min = 0;

		DBTuple next = child.next();
		while (!next.eof) {
			if (next.getFieldAsDouble(fieldNo) < min) {
				min = next.getFieldAsDouble(fieldNo);
			}
		}

		if (dt == DataType.INT) {
			return (int) Math.round(min);
		}

		return min;
	}

	private Object sum() {
		double sum = 0;

		DBTuple next = child.next();
		while (!next.eof) {
			sum += next.getFieldAsDouble(fieldNo);
		}

		if (dt == DataType.INT) {
			return (int) Math.round(sum);
		}

		return sum;
	}

	private int count() {
		int count = 0;

		DBTuple next = child.next();
		while (!next.eof) {
			count++;
			next = child.next();
		}

		return count;
	}
}
