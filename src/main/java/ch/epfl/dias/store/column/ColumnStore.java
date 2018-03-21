package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ColumnStore extends Store {

	private DataType[] schema;
	private Path path;
	private String delimiter;

	private List<DBColumn> cols;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this.schema = schema;
		this.path = Paths.get(filename);
		this.delimiter = delimiter;

		this.cols = new ArrayList<>();
	}

	@Override
	public void load() {
		cols.clear();

		List<List<Object>> temps = new ArrayList<>();
		for (int i = 0; i < schema.length; i++) {
			temps.add(i, new ArrayList<>());
		}

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

				for (int i = 0; i < fields.length; i++) {
					temps.get(i).add(fields[i]);
				}

				next_line = reader.readLine();
			}
		} catch (IOException ex) {
			return;
		}

		for (int i = 0; i < schema.length; i++) {
			cols.add(new DBColumn(temps.get(i), schema[i]));
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		if (columnsToGet == null) {
			DBColumn[] out = new DBColumn[cols.size()];

			for (int i = 0; i<cols.size(); i++) {
				out[i] = cols.get(i);
			}

			return out;
		} else {
			DBColumn[] out = new DBColumn[columnsToGet.length];

			for (int i = 0; i < columnsToGet.length; i++) {
				out[i] = cols.get(columnsToGet[i]);
			}

			return out;
		}
	}
}
