package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	private VolcanoOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple next = child.next();

		while(true) {
			if (next.eof) {
				return next;
			}

			switch (op) {
				case EQ:
					if (next.getFieldAsInt(fieldNo) == value) { return next; }
					break;
				case GE:
					if (next.getFieldAsInt(fieldNo) >= value) { return next; }
					break;
				case GT:
					if (next.getFieldAsInt(fieldNo) > value) { return next; }
					break;
				case LE:
					if (next.getFieldAsInt(fieldNo) <= value) { return next; }
					break;
				case LT:
					if (next.getFieldAsInt(fieldNo) < value) { return next; }
					break;
				case NE:
					if(next.getFieldAsInt(fieldNo) != value) { return next; }
			}

			next = child.next();
		}
	}

	@Override
	public void close() {
		child.close();
	}
}
