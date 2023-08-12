﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Transactions;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.SqlServer
{
    [TestFixture]
    public class Examples
    {
        #region TestFixtureSetUp

        private const string CreateSchemaSql = @"

/* Drop Tables */
IF ( EXISTS (	SELECT	* 
				FROM	INFORMATION_SCHEMA.TABLES 
				WHERE	TABLE_SCHEMA = 'dbo' 
						AND	TABLE_NAME = 'EmailAddress' ) )
BEGIN
	DROP TABLE EmailAddress
END

IF ( EXISTS (	SELECT	* 
				FROM	INFORMATION_SCHEMA.TABLES 
				WHERE	TABLE_SCHEMA = 'dbo' 
						AND	TABLE_NAME = 'Customer' ) )
BEGIN
	DROP TABLE Customer
END

/* Create Customer Table */
IF ( NOT EXISTS (	SELECT	* 
					FROM	INFORMATION_SCHEMA.TABLES 
					WHERE	TABLE_SCHEMA = 'dbo' 
							AND	TABLE_NAME = 'Customer') )
BEGIN

	CREATE TABLE Customer
	(
		CustomerId      INT             NOT NULL    IDENTITY(1,1),
		FirstName       NVARCHAR(120)   NOT NULL,
		LastName        NVARCHAR(120)   NOT NULL,
		DateOfBirth     DATETIME        NOT NULL,
		ModifiedDate	DATETIME		NOT NULL,
		ModifiedBy		NVARCHAR(120)	NOT NULL
	);

	ALTER TABLE Customer
	ADD CONSTRAINT [PK_Customer_CustomerId]
	PRIMARY KEY CLUSTERED ( CustomerId );

	ALTER TABLE Customer
	ADD CONSTRAINT [DF_Customer_ModifiedDate] 
	DEFAULT GETDATE() FOR ModifiedDate;

	ALTER TABLE Customer
	ADD CONSTRAINT [DF_Customer_ModifiedBy] 
	DEFAULT SUSER_SNAME() FOR ModifiedBy;

END


/* Create EmailAddress Table */
IF ( NOT EXISTS (	SELECT	* 
					FROM	INFORMATION_SCHEMA.TABLES 
					WHERE	TABLE_SCHEMA = 'dbo' 
							AND	TABLE_NAME = 'EmailAddress') )
BEGIN

	CREATE TABLE EmailAddress
	(
		EmailAddressId	INT             NOT NULL    IDENTITY(1,1),
		CustomerId      INT             NOT NULL,
		EmailAddress    NVARCHAR(256)   NOT NULL,
		IsActive		BIT		        NOT NULL,
		ModifiedDate	DATETIME		NOT NULL,
		ModifiedBy		NVARCHAR(120)	NOT NULL
	);

	ALTER TABLE EmailAddress
	ADD CONSTRAINT [PK_EmailAddress_EmailAddressId]
	PRIMARY KEY CLUSTERED ( EmailAddressId );

	ALTER TABLE EmailAddress
	ADD CONSTRAINT [FK_EmailAddress_CustomerId] 
	FOREIGN KEY ( CustomerId )
	REFERENCES Customer ( CustomerId );

	ALTER TABLE EmailAddress
	ADD CONSTRAINT [DF_EmailAddress_ModifiedDate] 
	DEFAULT GETDATE() FOR ModifiedDate;

	ALTER TABLE EmailAddress
	ADD CONSTRAINT [DF_EmailAddress_ModifiedBy] 
	DEFAULT SUSER_SNAME() FOR ModifiedBy;

	ALTER TABLE EmailAddress
	ADD CONSTRAINT [AK_EmailAddress_EmailAddress] 
	UNIQUE ( EmailAddress );

END";
        //[TestFixtureSetUp]
        public async void TestFixtureSetUp()
        {
            await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( CreateSchemaSql )
                .ExecuteNonQueryAsync();
        }

        #endregion TestFixtureSetUp

#region Execute Methods

[Test]
public async void ExecuteNonQuery_With_No_Parameters_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

/* This insert should trigger 1 row affected */
INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );";

    // Act
    int rowsAffected = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteNonQueryAsync();

    // Assert
    Assert.That( rowsAffected == 1 );
}

[Test]
public async void ExecuteNonQuery_With_Parameters_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( @SuperHeroName1 );

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( @SuperHeroName2 );";

    // Act
    int rowsAffected = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .AddParameter( "@SuperHeroName1", "Superman", DbType.AnsiString )
        .AddParameter( "@SuperHeroName2", "Batman", DbType.AnsiString )
        .ExecuteNonQueryAsync();

    // Assert
    Assert.That( rowsAffected == 2 );
}

[Test]
public async void ExecuteReader_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Batman' );

SELECT  SuperHeroId,
        SuperHeroName
FROM    #SuperHero;";

    List<object> list = new List<object>();

    // Act
    await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteReaderAsync( record =>
        {
            var obj = new
            {
                SuperHeroId = record.GetValue( 0 ),
                SuperHeroName = record.GetValue( 1 )
            };

            list.Add( obj );
        } );

    // Assert
    Assert.That( list.Count == 2 );
}

[Test]
public async void ExecuteScalar_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

SELECT  SuperHeroId, /* This should be the only value returned from ExecuteScalar */
        SuperHeroName
FROM    #SuperHero;";

    // Act
    int superHeroId = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteScalarAsync<int>(); // Using one of the many handy Sequelocity helper extension methods

    // Assert
    Assert.That( superHeroId == 1 );
}

[Test]
public async void ExecuteScalar_Of_Type_T_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

SELECT  SuperHeroId, /* This should be the only value returned from ExecuteScalar */
        SuperHeroName
FROM    #SuperHero;";

    // Act
    int superHeroId = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteScalarAsync<int>();

    // Assert
    Assert.That( superHeroId == 1 );
}

[Test]
public void DataSet_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Batman' );

SELECT  SuperHeroId,
        SuperHeroName
FROM    #SuperHero;";

    // Act
    DataSet dataSet = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteToDataSet();

    // Assert
    Assert.That( dataSet.Tables[0].Rows.Count == 2 );
    Assert.That( dataSet.Tables[0].Rows[0][0].ToString() == "1" );
    Assert.That( dataSet.Tables[0].Rows[0][1].ToString() == "Superman" );
    Assert.That( dataSet.Tables[0].Rows[1][0].ToString() == "2" );
    Assert.That( dataSet.Tables[0].Rows[1][1].ToString() == "Batman" );
}

[Test]
public void DataTable_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Batman' );

SELECT  SuperHeroId,
        SuperHeroName
FROM    #SuperHero;";

    // Act
    DataTable dataTable = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteToDataTable();

    // Assert
    Assert.That( dataTable.Rows.Count == 2 );
    Assert.That( dataTable.Rows[0][0].ToString() == "1" );
    Assert.That( dataTable.Rows[0][1].ToString() == "Superman" );
    Assert.That( dataTable.Rows[1][0].ToString() == "2" );
    Assert.That( dataTable.Rows[1][1].ToString() == "Batman" );
}

[Test]
public async void ExecuteToDynamicList_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Batman' );

SELECT  SuperHeroId,
        SuperHeroName
FROM    #SuperHero;";

    // Act
    List<dynamic> superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteToDynamicListAsync();

    // Assert
    Assert.That( superHeroes.Count == 2 );
    Assert.That( superHeroes[0].SuperHeroId == 1 );
    Assert.That( superHeroes[0].SuperHeroName == "Superman" );
    Assert.That( superHeroes[1].SuperHeroId == 2 );
    Assert.That( superHeroes[1].SuperHeroName == "Batman" );
}

[Test]
public async void ExecuteToDynamicObject_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Batman' );

SELECT  TOP 1
        SuperHeroId,
        SuperHeroName
FROM    #SuperHero;";

    // Act
    dynamic superHero = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteToDynamicObjectAsync();

    // Assert
    Assert.NotNull( superHero );
    Assert.That( superHero.SuperHeroId == 1 );
    Assert.That( superHero.SuperHeroName == "Superman" );
}

[Test]
public async void ExecuteToList_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
	SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
	SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Batman' );

SELECT  SuperHeroId,
		SuperHeroName
FROM    #SuperHero;";

    // Act
    List<SuperHero> superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteToListAsync<SuperHero>();

    // Assert
    Assert.That( superHeroes.Count == 2 );
    Assert.That( superHeroes[0].SuperHeroId == 1 );
    Assert.That( superHeroes[0].SuperHeroName == "Superman" );
    Assert.That( superHeroes[1].SuperHeroId == 2 );
    Assert.That( superHeroes[1].SuperHeroName == "Batman" );
}

[Test]
public async void ExecuteToMap_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Batman' );

SELECT  SuperHeroId,
        SuperHeroName
FROM    #SuperHero;";

    // Act
    List<SuperHero> superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteToMapAsync( record =>
        {
            var obj = new SuperHero
            {
                SuperHeroId = record.GetValue( 0 ).ToLong(),
                SuperHeroName = record.GetValue( 1 ).ToString()
            };

            return obj;
        } );

    // Assert
    Assert.That( superHeroes.Count == 2 );
    Assert.That( superHeroes[0].SuperHeroId == 1 );
    Assert.That( superHeroes[0].SuperHeroName == "Superman" );
    Assert.That( superHeroes[1].SuperHeroId == 2 );
    Assert.That( superHeroes[1].SuperHeroName == "Batman" );
}

[Test]
public async void ExecuteToObject_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Batman' );

SELECT  TOP 1
        SuperHeroId,
        SuperHeroName
FROM    #SuperHero;";

    // Act
    SuperHero superHero = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteToObjectAsync<SuperHero>();

    // Assert
    Assert.NotNull( superHero );
    Assert.That( superHero.SuperHeroId == 1 );
    Assert.That( superHero.SuperHeroName == "Superman" );
}

#endregion Execute Methods

#region Generate Insert Methods

public class Customer
{
    public int? CustomerId; // Setting the primary key as nullable
    public string FirstName;
    public string LastName;
    public DateTime DateOfBirth;
}

[Test]
public async void GenerateInsertForSqlServer_Example()
{
    // Arrange
    const string sql = @"
IF ( EXISTS (	SELECT	* 
				FROM	INFORMATION_SCHEMA.TABLES 
				WHERE	TABLE_SCHEMA = 'dbo' 
						AND	TABLE_NAME = 'Customer' ) )
BEGIN
	DROP TABLE Customer
END

IF ( NOT EXISTS (	SELECT	* 
					FROM	INFORMATION_SCHEMA.TABLES 
					WHERE	TABLE_SCHEMA = 'dbo' 
							AND	TABLE_NAME = 'Customer') )
BEGIN
	CREATE TABLE Customer
	(
		CustomerId      INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
		FirstName       NVARCHAR(120)   NOT NULL,
		LastName        NVARCHAR(120)   NOT NULL,
		DateOfBirth     DATETIME        NOT NULL
	);
END
";

    await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteNonQueryAsync();

    Customer customer = new Customer { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse( "06/18/1938" ) };

    // Act
    int customerId = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .GenerateInsertForSqlServer( customer )
        .ExecuteScalarAsync<int>();

    // Assert
    Assert.That( customerId == 1 );
}

[Test]
public async void GenerateInsertForSqlServer_Example_Using_An_Anonymous_Type()
{
    // Arrange
    const string sql = @"
CREATE TABLE #Customer
(
	CustomerId      INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
	FirstName       NVARCHAR(120)   NOT NULL,
	LastName        NVARCHAR(120)   NOT NULL,
	DateOfBirth     DATETIME        NOT NULL
);";

    DbConnection dbConnection = Sqlocity.CreateDbConnection( ConnectionStringsNames.SqlServerConnectionString );

    await Sqlocity.GetDatabaseCommand( dbConnection )
        .SetCommandText( sql )
        .ExecuteNonQueryAsync( true ); // Passing in 'true' to keep the connection open since this example is using a temp table which only exists during the scope / lifetime of this database connection

    // Anonymous Type
    var customer = new { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse( "06/18/1938" ) };

    // Act
    int customerId = await Sqlocity.GetDatabaseCommand( dbConnection )
        .GenerateInsertForSqlServer( customer, "#Customer" ) // Specifying table name since Sequelocity can't use the type name as the table name
        .ExecuteScalarAsync<int>();

    // Assert
    Assert.That( customerId == 1 );
}

[Test]
public async void GenerateInsertsForSqlServer_Example()
{
    // Arrange
    const string sql = @"
IF ( EXISTS (	SELECT	* 
				FROM	INFORMATION_SCHEMA.TABLES 
				WHERE	TABLE_SCHEMA = 'dbo' 
						AND	TABLE_NAME = 'Customer' ) )
BEGIN

	DROP TABLE Customer

END

IF ( NOT EXISTS (	SELECT	* 
					FROM	INFORMATION_SCHEMA.TABLES 
					WHERE	TABLE_SCHEMA = 'dbo' 
							AND	TABLE_NAME = 'Customer') )
BEGIN

	CREATE TABLE Customer
	(
		CustomerId      INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
		FirstName       NVARCHAR(120)   NOT NULL,
		LastName        NVARCHAR(120)   NOT NULL,
		DateOfBirth     DATETIME        NOT NULL
	);

END
";
    
    await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .ExecuteNonQueryAsync();

    Customer customer1 = new Customer { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse( "06/18/1938" ) };
    Customer customer2 = new Customer { FirstName = "Bruce", LastName = "Wayne", DateOfBirth = DateTime.Parse( "05/27/1939" ) };
    Customer customer3 = new Customer { FirstName = "Peter", LastName = "Parker", DateOfBirth = DateTime.Parse( "08/18/1962" ) };
    List<Customer> list = new List<Customer> { customer1, customer2, customer3 };

    // Act
    List<long> customerIds = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .GenerateInsertsForSqlServer( list )
        .ExecuteToListAsync<long>();

    // Assert
    Assert.That( customerIds.Count == 3 );
    Assert.That( customerIds[0] == 1 );
    Assert.That( customerIds[1] == 2 );
    Assert.That( customerIds[2] == 3 );
}

 #endregion Generate Insert Methods

#region Adding Parameter Methods

public class SuperHero
{
    public long SuperHeroId;
    public string SuperHeroName;
}


public async void AddParameter_Example()
{
    List<SuperHero> superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( "SELECT * FROM SuperHero WHERE SuperHeroName = @SuperHeroName" )
        .AddParameter( "@SuperHeroName", "Superman" )
        .ExecuteToListAsync<SuperHero>();
}

public async void AddParameter_Example_Specifying_An_Explicit_DbType()
{
    List<SuperHero> superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( "SELECT * FROM SuperHero WHERE SuperHeroName = @SuperHeroName" )
        .AddParameter( "@SuperHeroName", "Superman", DbType.AnsiString )
        .ExecuteToListAsync<SuperHero>();
}

public async void AddParameter_Example_Providing_A_DbParameter()
{
    DatabaseCommand databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString );

    var dbParameter = databaseCommand.DbCommand.CreateParameter();
    dbParameter.ParameterName = "SuperHeroName";
    dbParameter.Value = "Superman";
    dbParameter.Direction = ParameterDirection.InputOutput;

    List<SuperHero> superHeroes = await databaseCommand
        .SetCommandText( "SELECT * FROM SuperHero WHERE SuperHeroName = @SuperHeroName" )
        .AddParameter( dbParameter )
        .ExecuteToListAsync<SuperHero>();
}

public async void AddParameters_Example_Providing_A_List_Of_Parameter_Values_For_Use_In_An_IN_Clause()
{
    List<string> parameterList = new List<string> { "Superman", "Batman", "Spider-Man" };

    List<SuperHero> superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( "SELECT * FROM SuperHero WHERE SuperHeroName IN ( @SuperHeroNames )" )
        .AddParameters( "@SuperHeroNames", parameterList, DbType.AnsiString )
        .ExecuteToListAsync<SuperHero>();
}

public async void AddParameters_Example_Providing_A_Dictionary_Of_Parameter_Names_And_Values()
{
    const string sql = @"
SELECT  *
FROM    SuperHero
WHERE   SuperHeroId = @SuperHeroId
        OR SuperHeroName = @SuperHeroName
        OR SuperHeroName LIKE '%@SuperHeroPartialName%'";

    IDictionary<string, object> dictionary = new Dictionary<string, object>
    {
        { "@SuperHeroId", 1 },
        { "@SuperHeroName", "Superman" },
        { "@SuperHeroPartialName", "S" }
    };

    List<SuperHero> superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .AddParameters( dictionary )
        .ExecuteToListAsync<SuperHero>();
}

public async void AddParameters_Example_Supplying_A_Parameter_Array_Of_DbParameters()
{
    const string sql = @"
SELECT  *
FROM    SuperHero
WHERE   SuperHeroId = @SuperHeroId
        OR SuperHeroName = @SuperHeroName
        OR SuperHeroName LIKE '%@SuperHeroPartialName%'";

    DatabaseCommand databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString );

    DbParameter superHeroIdParameter = databaseCommand.CreateParameter( "@SuperHeroId", 1, DbType.Int32 );
    DbParameter superHeroNameParameter = databaseCommand.CreateParameter( "@SuperHeroName", "Superman", DbType.AnsiString );
    DbParameter superHeroPartialNameParameter = databaseCommand.CreateParameter( "@SuperHeroPartialName", "S", DbType.AnsiString );

    List<SuperHero> superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql )
        .AddParameters( superHeroIdParameter, superHeroNameParameter, superHeroPartialNameParameter )
        .ExecuteToListAsync<SuperHero>();
}

#endregion Adding Parameter Methods

#region Miscellaneous Helper Methods

// AppendCommandText

public void AppendCommandText_Example()
{
    const string sql = "SELECT TOP 1 * FROM SuperHero;";

    DatabaseCommand databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( sql );

    // Imagine there is some conditional logic here where we need to add additional queries to the database command
    const string moreSql = "\nSELECT TOP 1 * FROM AlterEgo;";

    databaseCommand.AppendCommandText( moreSql );
}

// CreateParameter
// SetCommandTimeout
// SetCommandType
// ToDatabaseCommand
// ToDbCommand

#endregion Miscellaneous Helper Methods

#region Transaction Examples

[Test]
public async void BeginTransaction_Example()
{
    const string sqlCommand1 = @"
CREATE TABLE #Customer
(
	CustomerId      INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
	FirstName       NVARCHAR(120)   NOT NULL,
	LastName        NVARCHAR(120)   NOT NULL,
	DateOfBirth     DATETIME        NOT NULL
);

INSERT INTO #Customer VALUES ( 'Clark', 'Kent', '06/18/1938' );
INSERT INTO #Customer VALUES ( 'Bruce', 'Wayne', '05/27/1939' );
";

    const string sqlCommand2 = @"
INSERT INTO #Customer VALUES ( 'Peter', 'Parker', '08/18/1962' );
";

    using ( var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString ) )
    {
        using ( var transaction = databaseCommand.BeginTransaction() )
        {
            var rowsUpdated = await databaseCommand
                .SetCommandText( sqlCommand1 )
                .ExecuteNonQueryAsync( keepConnectionOpen: true );

            var nextRowsUpdated = await databaseCommand
                .SetCommandText( sqlCommand2 )
                .ExecuteNonQueryAsync( keepConnectionOpen: true );

            Assert.That( rowsUpdated == 2 && nextRowsUpdated == 1 );

            if ( rowsUpdated == 2 && nextRowsUpdated == 1 )
                transaction.Commit();
        }
    }
}

[Test]
public async void TransactionScope_Example()
{
    const string sqlCommand1 = @"
IF ( EXISTS (	SELECT	* 
				FROM	INFORMATION_SCHEMA.TABLES 
				WHERE	TABLE_SCHEMA = 'dbo' 
						AND	TABLE_NAME = 'Customer' ) )
BEGIN
	DROP TABLE Customer
END

IF ( NOT EXISTS (	SELECT	* 
					FROM	INFORMATION_SCHEMA.TABLES 
					WHERE	TABLE_SCHEMA = 'dbo' 
							AND	TABLE_NAME = 'Customer') )
BEGIN
	CREATE TABLE Customer
	(
		CustomerId      INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
		FirstName       NVARCHAR(120)   NOT NULL,
		LastName        NVARCHAR(120)   NOT NULL,
		DateOfBirth     DATETIME        NOT NULL
	);
END

INSERT INTO Customer VALUES ( 'Clark', 'Kent', '06/18/1938' );
INSERT INTO Customer VALUES ( 'Bruce', 'Wayne', '05/27/1939' );
";

    const string sqlCommand2 = @"
INSERT INTO Customer VALUES ( 'Peter', 'Parker', '08/18/1962' );
";

    using ( var transaction = new TransactionScope() )
    {
        var rowsUpdated = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( sqlCommand1 )
                .ExecuteNonQueryAsync();

        var nextRowsUpdated = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
            .SetCommandText( sqlCommand2 )
            .ExecuteNonQueryAsync();

        Assert.That( rowsUpdated == 2 && nextRowsUpdated == 1 );

        if ( rowsUpdated == 2 && nextRowsUpdated == 1 )
            transaction.Complete();
    }
} 

#endregion Transaction Examples

#region Event Handler Examples

[Test]
public async void PreExecute_Example()
{
    // Arrange
    string commandText = string.Empty;

    Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPreExecuteEventHandlers.Add( command =>
    {
        if ( command.DbCommand.CommandType == CommandType.Text )
        {
            command.DbCommand.CommandText = "/* Application Name: MyAppName */" + Environment.NewLine + command.DbCommand.CommandText;
            commandText = command.DbCommand.CommandText;
        }
    } );

    // Act
    var id = await Sqlocity.GetDatabaseCommandForSqlServer( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( "SELECT 1 as Id" )
        .ExecuteScalarAsync<int>();

    // Visual Assertion
    Trace.WriteLine( commandText );

    // Assert
    Assert.That( commandText.StartsWith( "/* Application Name: MyAppName */" ) );
    Assert.That( id == 1 );

    // Cleanup
    Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPreExecuteEventHandlers.Clear();
}

[Test]
public async void PostExecute_Example()
{
    // Arrange
    var dictionary = new ConcurrentDictionary<DatabaseCommand,Stopwatch>();
    long elapsedMilliseconds = 0;

    Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPreExecuteEventHandlers.Add( command =>
    {
        dictionary[command] = Stopwatch.StartNew();
    } );

    Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPostExecuteEventHandlers.Add( command =>
    {
        Stopwatch stopwatch;
        if ( dictionary.TryRemove( command, out stopwatch ) )
            elapsedMilliseconds = stopwatch.ElapsedMilliseconds;
    } );

    // Act
    var id = await Sqlocity.GetDatabaseCommandForSqlServer( ConnectionStringsNames.SqlServerConnectionString )
        .SetCommandText( "SELECT 1 as Id" )
        .ExecuteScalarAsync<int>();

    // Visual Assertion
    Trace.WriteLine( "Elapsed Milliseconds: " + elapsedMilliseconds );

    // Assert
    Assert.That( elapsedMilliseconds >= 0 );

    // Cleanup
    Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPreExecuteEventHandlers.Clear();
    Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPostExecuteEventHandlers.Clear();
}

[Test]
public async void UnhandledException_Example()
{
    // Arrange
    Exception thrownException = null;

    Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandUnhandledExceptionEventHandlers.Add( ( exception, command ) =>
    {
        thrownException = exception;
    } );
            
    try
    {
        var id = await Sqlocity.GetDatabaseCommandForSqlServer( ConnectionStringsNames.SqlServerConnectionString )
            .SetCommandText( "SELECT asdasdffsdf as Id" )
            .ExecuteScalarAsync<int>();
    }
    catch ( Exception )
    {
        // ignored
    }

    // Visual Assertion
    Trace.WriteLine( thrownException );

    // Assert
    Assert.NotNull( thrownException.Message.Contains( "Invalid column name 'asdasdffsdf'" ) );

    // Cleanup
    Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandUnhandledExceptionEventHandlers.Clear();
}

#endregion Event Handler Examples
    }
}