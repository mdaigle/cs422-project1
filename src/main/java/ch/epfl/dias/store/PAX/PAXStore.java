package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {
	DataType[] schema;
	Path path;
	String delimiter;
	int tuplesPerPage;

	List<DBPAXpage> pages;

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.schema = schema;
		this.path = Paths.get(filename);
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
	}

	@Override
	public void load() {
		try (BufferedReader reader = Files.newBufferedReader(path)){
			String next_line = reader.readLine();

			int counter = 0;
			DBPAXpage currPage = new DBPAXpage(schema);

			while (next_line != null) {
				if (counter <= tuplesPerPage) {
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
					currPage.addTuple(fields);

					next_line = reader.readLine();
					counter++;
				} else {
					pages.add(currPage);
					currPage = new DBPAXpage(schema);
					counter = 0;
				}
			}
		} catch (IOException ex) {
			return;
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {
		int page_num = Math.floorDiv(rownumber, tuplesPerPage);
		if (page_num > pages.size()) {
			return new DBTuple();
		}

		DBPAXpage page = pages.get(page_num);
		return page.getRow(rownumber % tuplesPerPage);
	}
}
