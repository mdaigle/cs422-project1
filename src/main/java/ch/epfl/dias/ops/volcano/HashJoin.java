package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashJoin implements VolcanoOperator {

	private VolcanoOperator leftChild, rightChild;
	private int leftFieldNo, rightFieldNo;

	private Map<Object, List<DBTuple>> map;
	private List<DBTuple> toReturn;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.toReturn = new ArrayList<>();
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
	}

	@Override
	public DBTuple next() {
		if (map == null) {
			map = new HashMap<>();

			DBTuple nextLeft = leftChild.next();

			while (!nextLeft.eof) {
				Object key = nextLeft.fields[leftFieldNo];
				if (!map.containsKey(key)) {
					map.put(key, new ArrayList<>());
				}

				List<DBTuple> list = map.get(key);
				list.add(nextLeft);
				map.put(key, list);

				nextLeft = leftChild.next();
			}
		}

		if (!toReturn.isEmpty()) {
			return toReturn.remove(0);
		}

		DBTuple nextRight = rightChild.next();
		while (!nextRight.eof) {
			Object key = nextRight.fields[rightFieldNo];
			if (!map.containsKey(key)) {
				nextRight = rightChild.next();
				continue;
			}

			List<DBTuple> matches = map.get(key);
			for (DBTuple match : matches) {
				toReturn.add(match.join(nextRight));
			}

			return toReturn.remove(0);
		}

		return new DBTuple();
	}

	@Override
	public void close() {
		// TODO: Implement
	}
}
