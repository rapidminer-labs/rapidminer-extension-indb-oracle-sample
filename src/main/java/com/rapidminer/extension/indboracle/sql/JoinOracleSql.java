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

import com.rapidminer.extension.indatabase.db.step.Join;
import com.rapidminer.extension.indatabase.db.step.Join.JoinType;
import com.rapidminer.extension.indatabase.sql.JoinAnsiSql;


/**
 * Class demonstrating how to customize the SQL syntax for Join operation.
 *
 * @author Peter Hellinger
 */
public class JoinOracleSql extends JoinAnsiSql {

	@Override
	protected String buildJoinTypeExpression(Join join) {
		if (JoinType.OUTER == join.getType()) {
			return "FULL OUTER JOIN";
		} else {
			return super.buildJoinTypeExpression(join);
		}
	}

}
