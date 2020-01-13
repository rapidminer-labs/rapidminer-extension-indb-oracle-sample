Extending RapidMiner In-Database Processing extension
=====================================================

What is In-Database Processing extension?
-----------------------------------------

[In-Database Processing extension](https://marketplace.rapidminer.com/UpdateServer/faces/product_details.xhtml?productId=rmx_in_database_processing) is a RapidMiner extension in which you can visually define data prep or ETL workflows and execute them directly in a database. It can drastically reduce data transfer by loading only the data you need after preparation. This is especially important in case of cloud engines where you may pay for the bandwidth.

The extension brings built-in support for MySQL, PostgreSQL and Google BigQuery. But it is designed in a way to allow you to extend its capabilities via developing a regular RapidMiner extension. You can add new database support, or even add custom operators.

What is this sample project about?
----------------------------------

This a just a sample project. It aims to show you how to extend the database support and the operator set that the In-Database Processing extension provides. It uses the well-defined concepts of that extension to help adding database-specific code as simply as possible. This project also works as a regular RapidMiner extension that can be used together with the original In-Database Processing extension. Note that to be able to use this extension, you need to enable **Grand additional permissions to unsigned extensions** under **Preferences** / **Start-up** (requires a Studio restart after changing the setting, see the [documentation for details](https://docs.rapidminer.com/latest/developers/extensions/security/#permissions)).

The goal here is to allow partners, customers to extend the In-Database Processing capibilities to also cover their RDBMS/cloud engine where they may have more expertise than anyone else. You are also encouraged to join the community and share your extension with others as well.

Disclaimer
----------

Do NOT use this extension in production. It is just a sample that provides an example on how to customize the behaviour of In-Database Processing extension for your database. It does not add support for Oracle databases, only shows you how you can start a similar project. Also, see incomplete list of known issues below.

How to extend In-Database Processing capabilities?
--------------------------------------------------

This section goes deeper into the structure and the classes that your code can use to implement database-specific behaviour or custom operators. Note that the JavaDoc for the latest version is [available here](https://rapidminer-labs.github.io/indb/apidocs/).

Database-specific customization is done via the *DatabaseProvider* and related classes. *DatabaseProvider* answers basic syntax-related questions about your database (e.g. what quote character or data format it uses) and implements basic metadata retrieval methods (how to list schemas, tables, columns). The customization can be done by overriding the default function implementations of *DatabaseProvider* interface - when needed, i.e. default implementation is not good for your database. We have published test processes that help you validate that your *DatabaseProvider* implementation works as expected, see below.

Database-specific SQL syntax can be implemented via registering new *SqlSyntax* instances in the *DatabaseProvider*, see an example for that in this project.

Database-specific (stored) functions can be described using a resource file (*.properties*). If this file is registered during plugin initialization properly, **Generate Attributes** operator will show the functions in its **Expression Editor** dialog. The default aggregation function list of **Aggregate** operator can also be modified by overriding *DatabaseProvider#getAggregationFunctions()*.

New operators can be implemented similarly to how other RapidMiner extensions add new operators. In this case, however, *AbstractNestedOperator* should be extended to add a new In-Database Processing operator (let's simply call such an operator an "in-database operator"). These operators always run inside a **Database Nest** metaoperator. They describe how the database should perform the step that this they represent.

The following section helps you in how to go deeper in any of these aspects. Here, we already assume that you are somewhat familiar how a RapidMiner extension's code works together with the core code, and how an operator is implemented. See [How to Extend RapidMiner](https://docs.rapidminer.com/developers/) developer guide.

Source code overview
--------------------
In-database operators are responsible for translating themselves to *DbStep* objects. *DbStep* objects point to previous one or more *DbStep* objects (their input), so there is no need for any global entity to maintain a list or chain explicitly. A *DbStep* is like a step in a query execution plan, examples are *Join*, *Union*, etc.
*DbStep* objects travel on the wire between operator ports: the *DbTableExampleSet* – an *IOObject* – refers to the *DbStep* object that creates it. In the end, at the output port of the Nest, the *DbStep* chain describes (contains all required information for) the work that the database query will need to perform. (No need to look at operators again.)

There should be one *SqlSyntax* object registered for each *DbStep* types (hence, the generics: *SqlSyntax\<T extends DbStep\>*) per each particular database provider. An *SqlSyntax* object is responsible for translating a *DbStep* to SQL code. Default implementations (usually with "AnsiSql" suffix, e.g. *JoinAnsiSql*) are available, but database providers must replace all those *SqlSyntax* objects, where their syntax differs from the ANSI version (e.g.: *JoinMySql* overrides how a FULL OUTER JOIN is done in case of MySQL).
For simplicity, the *DbStep* class also has a *toSql()* method to do the (recursive) translation. It requires the database provider as an argument, and simply delegates the task to that provider, which delegates the job to the registered *SqlSyntax* object. This design leads to small and simple classes. *DbStep* objects are immutable. With *Lombok.Builder*, their creation is done via readable, SQL-like, declarative code.

Another huge simplification is achieved by avoiding the duplication of data transformation logic in operators by reusing the same code for metadata and data (*ExampleSet*) transformation.
This requires the following assumption: there is no in-database operator where output columns are only known during execution time (e.g., there is no Pivot). This is true for all current in-database operators and for those that are planned in short-term. 
This makes it possible that *DbStep* objects can also define a *getColumns()* method that is both used 1) during metadata validation to tell how output metadata looks like and 2) during execution time, how to assemble the SELECT clause in SQL code generation (*toSql()* method can use it).
This way, in-database operator code can also be simple and short - in fact, they are way simpler than their equivalent core operator counterparts. (Hence, there is also no need for them to extend core operators.) Major parts are just 1) operator parameter definition and 2) creation of the *DbStep* object using the builder pattern.

Because design-time and runtime code (creating *DbStep*) is typically the same, error handling required the introduction of *UserOrSetupError*, a class that can define both design-time (*MetaDataError*, *ProcessSetupError*) and runtime (*UserError*) errors – the caller code can then unwrap the appropriate one.

Test processes
--------------
We provide test processes with expected results that you can use to validate the extension you are developing. You can download these test processes from [this link](https://maven.rapidminer.com/service/local/repositories/releases/content/com/rapidminer/extension/in_database_processing/9.1.0/in_database_processing-9.1.0-process-tests.zip). The compressed file contains the directories *Phase1* and *Phase2*. The easiest thing is to open the folder where you uncompressed the file as a local repository in Studio. After that it is recommended to install [Process Testing extension](https://marketplace.rapidminer.com/UpdateServer/faces/product_details.xhtml?productId=rmx_test) from the Marketplace.

You need to create a database connection in Studio called *extension_indatabase* pointing to the database for which you are adding support. Use the *load_data* process from *Phase1* directory to load test data into your database. If that process succeeds, you can use the test processes from *Phase2* to check if your extension works as expected. You can run them manually one by one, or you can run them all by right clicking on the *Phase2* directory and choosing **Run as Test** option (available once you install Process Testing extension). You will get the result (success or failure) for all processes on the **Process Tests** panel.

Known issues
------------
This extension does not provide proper support for Oracle databases. It does not provide help for using Oracle's functions or special features. Note the following incomplete list of known issues:
- data (column) types do not work as expected, meaning that operators give different results in terms of data types than the officially supported MySQL, PostgreSQL and BigQuery engines - this is to be investigated in the near future


