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
package com.rapidminer.extension.indboracle.operator;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.rapidminer.example.ExampleSet;
import com.rapidminer.extension.indatabase.data.DbTableExampleSet;
import com.rapidminer.extension.indatabase.db.object.Column;
import com.rapidminer.extension.indatabase.db.object.ColumnExpr;
import com.rapidminer.extension.indatabase.db.step.DbStep;
import com.rapidminer.extension.indatabase.db.step.Select;
import com.rapidminer.extension.indatabase.exceptions.ConnectionEntryNotFound;
import com.rapidminer.extension.indatabase.exceptions.NestNotFoundException;
import com.rapidminer.extension.indatabase.exceptions.UserOrSetupError;
import com.rapidminer.extension.indatabase.metadata.DbTableMetaData;
import com.rapidminer.extension.indatabase.operator.AbstractNestedOperator;
import com.rapidminer.extension.indatabase.provider.DatabaseProvider;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.ProcessSetupError.Severity;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.metadata.SimpleMetaDataError;
import com.rapidminer.operator.tools.AttributeSubsetSelector;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.Ontology;


/**
 * Kind of dummy operator demonstrating extensibility of In-Database Processing Extension. It is
 * complete, however, because it defines all design-time and runtime error handling, parameters,
 * etc.
 *
 * @author Peter Hellinger
 */
public class Trim extends AbstractNestedOperator {

	private final AttributeSubsetSelector attributeSelector = new DbColumnSubsetSelector(this, getInputPort());

	/**
	 * @param description
	 */
	public Trim(OperatorDescription description) {
		super(description, true);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();
		types.addAll(attributeSelector.getParameterTypes());
		return types;
	}

	@Override
	public DbStep buildDbStep(DbStep... inputs)
			throws UndefinedParameterError, NestNotFoundException, UserOrSetupError, ConnectionEntryNotFound {

		DbStep input = inputs[0];
		DatabaseProvider provider = getProvider();
		ExampleSet inputEs = new DbTableExampleSet(provider, input);
		Set<String> attributeSubset;
		try {
			attributeSubset = attributeSelector.getAttributeSubset(inputEs, true).stream().map(a -> a.getName())
					.collect(Collectors.toSet());
		} catch (UserError e) {
			throw new UserOrSetupError().withUserError(e);
		}

		// trim nominal attribute values (in the selected attribute set), keep all other attributes
		Set<String> selectedNominals = input.getColumns(provider).stream()
				// we need to allow ATTRIBUTE_VALUE type here as well because of columns generated
				// by Generate Attributes
				.filter(c -> attributeSubset.contains(c.getDestCol()))
				.filter(c -> Ontology.ATTRIBUTE_VALUE_TYPE.isA(DbTableMetaData.getRapidMinerTypeIndex(c.getType()),
						Ontology.NOMINAL) || c.getType() == Ontology.ATTRIBUTE_VALUE)
				.map(c -> c.getDestCol()).collect(Collectors.toSet());
		if (selectedNominals.isEmpty()) {
			throw new UserOrSetupError().withUserError(new UserError(this, "no_nominals"))
					.withMetaDataError(new SimpleMetaDataError(Severity.ERROR, getInputPort(), "no_nominals"));
		}

		List<Column> outputColumns = input.getColumnRefs(provider).stream().map(c -> {
			if (selectedNominals.contains(c.getDestCol())) {
				// cast to String in case of unknown type
				return new ColumnExpr(String.format(
						c.getType() == Ontology.ATTRIBUTE_VALUE ? "TRIM(CAST (%s AS VARCHAR2(4000)))" : "TRIM(%s)",
						provider.quote(c.getDestCol())), c.getDestCol(), c.getType());
			} else {
				return c;
			}
		}).collect(Collectors.toList());
		return Select.builder().from(input).columns(outputColumns).build();
	}

}
