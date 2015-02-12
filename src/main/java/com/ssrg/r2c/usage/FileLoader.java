package com.ssrg.r2c.usage;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ssrg.r2c.usage.sql.Query;
import com.ssrg.r2c.usage.sql.QueryAttribute;
import com.ssrg.r2c.usage.sql.QueryFilter;


public class FileLoader {

	private static Pattern queryPattern = Pattern
			.compile("^SELECT\\s*\\(([A-Za-z0-9_.,\\s]*)\\)\\s*FROM\\s*\\(([A-Za-z0-9_.,\\s]*)\\)\\s*WHERE\\s*\\(([A-Za-z0-9_.,\\s]*)\\)");

	public static List<Query> loadQueriesFromFile(String filePath) {
		List<Query> queries = new ArrayList<Query>();
		InputStream fis = null;
		BufferedReader reader = null;

		try {
			//fis = ClassLoader.getSystemResourceAsStream(filePath);
			fis = new FileInputStream(filePath);
			reader = new BufferedReader(new InputStreamReader(fis));

			String line;
			while ((line = reader.readLine()) != null) {
				if (line.trim().length() > 0 && !line.startsWith("#")) {

					Query query = parseQuery(line);
					if (query != null) {
						queries.add(query);
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fis != null)
					fis.close();
				if (reader != null)
					reader.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		return queries;
	}

	private static Query parseQuery(String query) {
		Matcher m = queryPattern.matcher(query);

		if (m.matches()) {
			String select = m.group(1);
			String from = m.group(2);
			String where = m.group(3);

			// Select
			List<QueryAttribute> projections = parseSelect(select);
			List<String> relations = parseFrom(from);
			Set<QueryFilter> selections = parseWhere(where);

			return new Query(projections, relations, selections, 1);
		}

		return null;
	}

	private static List<QueryAttribute> parseSelect(String select) {
		List<QueryAttribute> result = new ArrayList<QueryAttribute>();

		String[] selectPieces = select.split(",");
		for (int i = 0; i < selectPieces.length; i++) {
			String[] tableColumn = selectPieces[i].split("\\.");

			if (tableColumn.length == 2) {
				QueryAttribute qa = new QueryAttribute(tableColumn[0].trim(),
						tableColumn[1].trim());
				result.add(qa);
			}
		}

		return result;
	}

	private static List<String> parseFrom(String from) {
		List<String> result = new ArrayList<String>();

		String[] fromPieces = from.split(",");
		for (int i = 0; i < fromPieces.length; i++) {
			result.add(fromPieces[i].trim());
		}

		return result;
	}

	private static Set<QueryFilter> parseWhere(String where) {
		Set<QueryFilter> result = new HashSet<QueryFilter>();

		String[] wherePieces = where.split(",");
		for (int i = 0; i < wherePieces.length; i++) {
			String[] tableColumn = wherePieces[i].split("\\.");

			if (tableColumn.length == 2) {
				QueryAttribute qa = new QueryAttribute(tableColumn[0].trim(),
						tableColumn[1].trim());
				result.add(new QueryFilter(qa));
			}
		}

		return result;
	}
}
