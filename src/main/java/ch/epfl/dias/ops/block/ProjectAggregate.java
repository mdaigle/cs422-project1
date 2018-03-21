package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.Arrays;
import java.util.Collections;

public class ProjectAggregate implements BlockOperator {

	private BlockOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	
	public ProjectAggregate(BlockOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
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
				return null;
		}

		return new DBColumn[]{new DBColumn(Collections.singletonList(val), dt)};
	}

	private Object average() {
		double sum = 0;

		DBColumn[] cols = child.execute();
		Double[] agg_col = cols[fieldNo].toDoubleArray();

		if (agg_col.length == 0) {
			return null;
		}

		for (Double val : agg_col) {
			sum += val;
		}

		return sum / agg_col.length;
	}

	private Object max() {
		DBColumn[] cols = child.execute();
		Double[] agg_col = cols[fieldNo].toDoubleArray();

		if (agg_col.length == 0) {
			return null;
		}

		double max = agg_col[0];
		for (Double val : agg_col) {
			if (val > max) {
				max = val;
			}
		}

		if (dt == DataType.INT) {
			return (int) Math.round(max);
		}

		return max;
	}

	private Object min() {
		DBColumn[] cols = child.execute();
		Double[] agg_col = cols[fieldNo].toDoubleArray();

		if (agg_col.length == 0) {
			return null;
		}

		double min = agg_col[0];
		for (Double val : agg_col) {
			if (val < min) {
				min = val;
			}
		}

		if (dt == DataType.INT) {
			return (int) Math.round(min);
		}

		return min;
	}

	private Object sum() {
		double sum = 0;

		DBColumn[] cols = child.execute();
		Double[] agg_col = cols[fieldNo].toDoubleArray();

		if (agg_col.length == 0) {
			return null;
		}

		for (Double val : agg_col) {
			sum += val;
		}

		if (dt == DataType.INT) {
			return (int) Math.round(sum);
		}

		return sum;
	}

	private int count() {
		DBColumn[] cols = child.execute();
		return cols[fieldNo].size();
	}
}
