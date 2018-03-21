package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements BlockOperator {

	private BlockOperator leftChild, rightChild;
	private int leftFieldNo, rightFieldNo;

	public Join(BlockOperator leftChild, BlockOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	public DBColumn[] execute() {
		Map<Object, List<Integer>> map = new HashMap<>();
		DBColumn[] leftCols = leftChild.execute(), rightCols = rightChild.execute();

		DBColumn[] out = new DBColumn[leftCols.length + rightCols.length];
		List<List<Object>> tempVals = new ArrayList<>();
		for (int i = 0; i < out.length; i++) {
			tempVals.add(new ArrayList<>());
		}

		List<Object> leftVals = leftCols[leftFieldNo].getVals();
		List<Object> rightVals = rightCols[rightFieldNo].getVals();



		// Loop through left join col and add to map
		for (int i = 0; i < leftVals.size(); i++) {
			Object key = leftVals.get(i);

			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<>());
			}

			List<Integer> list = map.get(key);
			list.add(i);
			map.put(key, list);
		}

		// Loop through right join col and find matches
		for (int i = 0; i < rightVals.size(); i++) {
			Object key = rightVals.get(i);

			if (!map.containsKey(key)) {
				continue;
			}

			List<Integer> match_indices = map.get(key);
			for (Integer match_index : match_indices) {
				for (int j = 0; j < leftCols.length; j++) {
					List<Object> tempArr = tempVals.get(j);
					tempArr.add(leftCols[j].getVals().get(match_index));
				}

				for (int j = 0; j < rightCols.length; j++) {
					if (tempVals.get(j + leftCols.length) == null) {
						tempVals.add(j + leftCols.length, new ArrayList<>());
					}

					List<Object> tempArr = tempVals.get(j + leftCols.length);
					tempArr.add(rightCols[j].getVals().get(i));
				}
			}
		}

		// Build DBColumns from value arrays
		for (int j = 0; j < leftCols.length; j++) {
			out[j] = new DBColumn(tempVals.get(j), leftCols[j].getType());
		}

		for (int j = 0; j < rightCols.length; j++) {
			out[j + leftCols.length] = new DBColumn(tempVals.get(j + leftCols.length), rightCols[j].getType());
		}

		return out;
	}
}
