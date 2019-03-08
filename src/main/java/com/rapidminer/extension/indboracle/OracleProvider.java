/**
 * This file is part of the In-Database Processing Sample project.
 *
 * Copyright (C) 2018-2019 RapidMiner GmbH
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see https://www.gnu.org/licenses/.
 */
package com.rapidminer.extension.indboracle;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.rapidminer.example.Attribute;
import com.rapidminer.extension.indatabase.DbTools;
import com.rapidminer.extension.indatabase.db.step.DbStep;
import com.rapidminer.extension.indatabase.db.step.Join;
import com.rapidminer.extension.indatabase.operator.function.FunctionDefinition;
import com.rapidminer.extension.indatabase.provider.DatabaseProvider;
import com.rapidminer.extension.indatabase.provider.other.GenericProvider;
import com.rapidminer.extension.indatabase.sql.SqlSyntax;
import com.rapidminer.extension.indboracle.sql.JoinOracleSql;
import com.rapidminer.extension.jdbc.tools.jdbc.DatabaseHandler;


/**
 * Customizes some of the default provider methods to demonstrate setting the ID, describing custom
 * aggregation functions and implementing metadata retrieval methods.
 *
 * @author Peter Hellinger
 */
public enum OracleProvider implements DatabaseProvider {

	INSTANCE;

	private static final Map<String, FunctionDefinition> AGGREGATEFUNCTIONS = new LinkedHashMap<>();

	static {
		// add all functions from generic provider and an Oracle-specific one (median) - still
		// keeping alphabetical ordering
		Map<String, FunctionDefinition> aggregateFunctions = GenericProvider.INSTANCE.getAggregationFunctions();
		FunctionDefinition[] fs = new FunctionDefinition[] { new FunctionDefinition("MEDIAN", "median",
				"Return the median value of the argument.", FunctionDefinition.OUTPUT_TYPE_SAME_AS_INPUT) };
		Stream.of(fs).forEachOrdered(f -> aggregateFunctions.put(f.getName(), f));
		aggregateFunctions.keySet().stream().sorted()
				.forEachOrdered(f -> AGGREGATEFUNCTIONS.put(f, aggregateFunctions.get(f)));
	}

	@Override
	public Map<String, FunctionDefinition> getAggregationFunctions() {
		return AGGREGATEFUNCTIONS;
	}

	@Override
	public String getId() {
		return "oracle";
	}

	@Override
	public List<String> getTableNames(DatabaseHandler internalDBHandler, String schemaName) throws SQLException {
		List<String> tableList = new ArrayList<>();

		String sqlQuery = "SELECT table_name FROM all_tables WHERE owner = ?";
		LOGGER.fine(String.format("Finding tables in schema '%s'", schemaName));
		try (PreparedStatement st = internalDBHandler.createPreparedStatement(sqlQuery, false)) {
			st.setString(1, schemaName);
			try (ResultSet rs = st.executeQuery()) {
				while (rs.next()) {
					tableList.add(rs.getString(1));
				}
			}
		}
		LOGGER.fine(String.format("Done finding tables in schema '%s'", schemaName));
		return DbTools.sortedList(tableList);
	}

	@Override
	public List<Attribute> getColumnMetaData(DatabaseHandler internalDBHandler, String schemaName, String tableName)
			throws SQLException {
		// LIMIT clause is not supported by Oracle
		String sqlQuery = "SELECT * FROM " + quote(schemaName) + "." + quote(tableName) + " WHERE 0=1";
		LOGGER.fine(String.format("Finding columns in table '%s'.'%s'", schemaName, tableName));
		List<Attribute> attrs;
		try (Statement st = internalDBHandler.createStatement(false, false); ResultSet rs = st.executeQuery(sqlQuery)) {
			attrs = createAttributes(rs);
		}
		LOGGER.fine(String.format("Done finding columns in table '%s'.'%s'", schemaName, tableName));
		return attrs;
	}

	@Override
	public Map<Class<? extends DbStep>, SqlSyntax<?>> getDbStepToSyntaxMap() {
		Map<Class<? extends DbStep>, SqlSyntax<?>> res = DatabaseProvider.super.getDbStepToSyntaxMap();
		res.put(Join.class, new JoinOracleSql());
		return res;
	}
}
