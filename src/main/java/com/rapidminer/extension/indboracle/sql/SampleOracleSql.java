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
package com.rapidminer.extension.indboracle.sql;

import com.rapidminer.extension.indatabase.db.step.Sample;
import com.rapidminer.extension.indatabase.provider.DatabaseProvider;
import com.rapidminer.extension.indatabase.sql.SqlSyntax;


/**
 * LIMIT clause is not part of any ANSI standard. Implementation for other databases available at
 * https://www.jooq.org/doc/3.11/manual/sql-building/sql-statements/select-statement/limit-clause/.
 * Using a syntax that works on Oracle versions before 12c and the OFFSET syntax.
 *
 * @author Peter Hellinger
 *
 */
public class SampleOracleSql implements SqlSyntax<Sample> {

	private static final String TEMPLATE = //
			"SELECT %s " + //
					"FROM (" + //
					"SELECT %s, ROWNUM %s " + //
					"FROM (%s) %s WHERE ROWNUM <= %d) %s " + //
					"WHERE %s > %d";

	@Override
	public String toSql(DatabaseProvider provider, Sample sample) {
		String columns = sample.getColumnList(provider);
		// Ideally, we should make sure here that no column with this name exists in the dataset.
		// Once a helper function for that becomes available, we can start to use it.
		String rownumAlias = provider.quote("rownumalias");
		return String.format(TEMPLATE, //
				columns, //
				columns, //
				rownumAlias, //
				sample.getFrom().toSql(provider), //
				provider.quote("t1"), //
				sample.getLimit() + sample.getOffset(), //
				provider.quote("t1"), //
				rownumAlias, //
				sample.getOffset());
	}
}
