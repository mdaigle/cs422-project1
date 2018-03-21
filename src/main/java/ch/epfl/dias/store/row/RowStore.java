package ch.epfl.dias.store.row;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class RowStore extends Store {
	DataType[] schema;
	Path path;
	String delimiter;
	List<DBTuple> rows;

	public RowStore(DataType[] schema, String filename, String delimiter) {
		this.schema = schema;
		this.path = Paths.get(filename);
		this.delimiter = delimiter;
		this.rows = new ArrayList<>();
	}

	@Override
	public void load() {
		try (BufferedReader reader = Files.newBufferedReader(path)){
			String next_line = reader.readLine();
			while (next_line != null) {
				String[] fieldStrs = next_line.split(delimiter);

				Object[] fields = new Object[fieldStrs.length];
				
				for (int i = 0; i < fieldStrs.length; i++) {
					switch (schema[i]) {
						case INT:
							fields[i] = Integer.parseInt(fieldStrs[i]);
							break;
						case STRING:
							fields[i] = fieldStrs[i];
							break;
						case DOUBLE:
							fields[i] = Double.parseDouble(fieldStrs[i]);
							break;
						case BOOLEAN:
							fields[i] = Boolean.parseBoolean(fieldStrs[i]);
							break;
					}
				}

				rows.add(new DBTuple(fields, schema));

				next_line = reader.readLine();
			}
		} catch (IOException ex) {
			return;
		}
		rows.add(new DBTuple());
	}

	@Override
	public DBTuple getRow(int rownumber) {
		if (rownumber >= rows.size()) {
			return new DBTuple();
		}

		return rows.get(rownumber);
	}
}
