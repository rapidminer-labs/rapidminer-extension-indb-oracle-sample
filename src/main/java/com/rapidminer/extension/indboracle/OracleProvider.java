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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import com.rapidminer.RapidMiner;
import com.rapidminer.example.Attribute;
import com.rapidminer.extension.indatabase.DbTools;
import com.rapidminer.extension.indatabase.db.CachedDatabaseHandler;
import com.rapidminer.extension.indatabase.db.object.Column;
import com.rapidminer.extension.indatabase.db.step.DbStep;
import com.rapidminer.extension.indatabase.db.step.Filter.FilterCondition;
import com.rapidminer.extension.indatabase.db.step.Join;
import com.rapidminer.extension.indatabase.db.step.Sample;
import com.rapidminer.extension.indatabase.metadata.DbTableMetaData;
import com.rapidminer.extension.indatabase.operator.function.FunctionDefinition;
import com.rapidminer.extension.indatabase.provider.DatabaseProvider;
import com.rapidminer.extension.indatabase.provider.QueryRunner;
import com.rapidminer.extension.indatabase.provider.other.GenericProvider;
import com.rapidminer.extension.indatabase.sql.SqlSyntax;
import com.rapidminer.extension.indboracle.sql.JoinOracleSql;
import com.rapidminer.extension.indboracle.sql.SampleOracleSql;
import com.rapidminer.extension.jdbc.tools.jdbc.DatabaseHandler;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.ParameterService;


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
	public List<String> getTableNames(CachedDatabaseHandler handler, String schemaName) throws SQLException, UserError {
		List<String> tableList = new ArrayList<>();

		boolean onlyStandardTables = Boolean
				.parseBoolean(ParameterService.getParameterValue(RapidMiner.PROPERTY_RAPIDMINER_TOOLS_DB_ONLY_STANDARD_TABLES));
		String sqlQuery = "SELECT object_name FROM all_objects WHERE owner = ? AND OBJECT_TYPE IN (%s)";
		if (onlyStandardTables) {
			sqlQuery = String.format(sqlQuery, literal("TABLE"));
		} else {
			sqlQuery = String.format(sqlQuery, literal("TABLE") + ", " + literal("VIEW"));
		}
		LOGGER.fine(String.format("Finding tables in schema '%s'", schemaName));
		try (DatabaseHandler dbHandler = handler.getConnectedDatabaseHandler();
				PreparedStatement st = dbHandler.createPreparedStatement(sqlQuery, false)) {
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
	public List<Attribute> getColumnMetaData(CachedDatabaseHandler handler, String schemaName, String tableName)
			throws SQLException, UserError {
		// LIMIT clause is not supported by Oracle
		String sqlQuery = "SELECT * FROM " + quote(schemaName) + "." + quote(tableName) + " WHERE 0=1";
		LOGGER.fine(String.format("Finding columns in table '%s'.'%s'", schemaName, tableName));
		List<Attribute> attrs = new ArrayList<>();
		try (QueryRunner queryRunner = createQueryRunner(handler)) {
			queryRunner.executeQuery(sqlQuery).createExampleTable(null).build().getAttributes().allAttributes()
					.forEachRemaining(attrs::add);
		} catch (OperatorException e) {
			// TODO: declare throwing OperatorException when the API supports it
			throw new SQLException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		LOGGER.fine(String.format("Done finding columns in table '%s'.'%s'", schemaName, tableName));
		return attrs;
	}

	@Override
	public Map<Class<? extends DbStep>, SqlSyntax<?>> getDbStepToSyntaxMap() {
		Map<Class<? extends DbStep>, SqlSyntax<?>> res = DatabaseProvider.super.getDbStepToSyntaxMap();
		res.put(Join.class, new JoinOracleSql());
		res.put(Sample.class, new SampleOracleSql());
		return res;
	}

	@Override
	public Map<FilterCondition, BiFunction<String, String, String>> getFilterSyntax() {
		Map<FilterCondition, BiFunction<String, String, String>> res = DatabaseProvider.super.getFilterSyntax();

		// no default escape character in Oracle, need to explicitly set it
		res.put(FilterCondition.CONTAINS,
				(col, val) -> col + " LIKE " + literal("%" + escapeLikeExpr(val) + "%") + " ESCAPE '\\'");
		res.put(FilterCondition.DOES_NOT_CONTAIN,
				(col, val) -> col + " NOT LIKE " + literal("%" + escapeLikeExpr(val) + "%") + " ESCAPE '\\'");
		res.put(FilterCondition.STARTS_WITH,
				(col, val) -> col + " LIKE " + literal(escapeLikeExpr(val) + "%") + " ESCAPE '\\'");
		res.put(FilterCondition.ENDS_WITH,
				(col, val) -> col + " LIKE " + literal("%" + escapeLikeExpr(val)) + " ESCAPE '\\'");

		// need to use REGEXP_LIKE function
		res.put(FilterCondition.MATCHES, (col, val) -> "REGEXP_LIKE(" + col + ", " + literal("^" + val + "$") + ")");
		return res;
	}

	@Override
	public boolean supportsDropIfExistsSyntax() {
		return false;
	}

	@Override
	public String format(String val, Column c) {
		// only format date related objects
		int type = DbTableMetaData.getRapidMinerTypeIndex(c.getType());
		if (Ontology.ATTRIBUTE_VALUE_TYPE.isA(type, Ontology.DATE_TIME)) {
			String dateStr = literal(DbTools.defaultFormat(val, type));
			String dateFormatOracle;
			if (Ontology.ATTRIBUTE_VALUE_TYPE.isA(type, Ontology.DATE)) {
				dateFormatOracle = "YYYY-MM-DD";
			} else if (Ontology.ATTRIBUTE_VALUE_TYPE.isA(type, Ontology.TIME)) {
				dateFormatOracle = "hh24:mi:ss";
			} else {
				dateFormatOracle = "YYYY-MM-DD hh24:mi:ss";
			}
			val = "TO_DATE(" + dateStr + ", '" + dateFormatOracle + "')";
		}
		return val;
	}
}
