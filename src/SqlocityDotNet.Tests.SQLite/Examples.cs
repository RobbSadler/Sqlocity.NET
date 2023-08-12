using System;
using System.Collections.Generic;
using System.Data.Common;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.SQLite
{
    [TestFixture]
    public class Examples
    {

public class Customer
{
    public int? CustomerId; // Setting the primary key as nullable
    public string FirstName;
    public string LastName;
    public DateTime DateOfBirth;
}

[Test]
public async void GenerateInsertForSQLite_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE IF NOT EXISTS Customer
(
    CustomerId      INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
    FirstName       NVARCHAR(120)   NOT NULL,
    LastName        NVARCHAR(120)   NOT NULL,
    DateOfBirth     DATETIME        NOT NULL
);";

    DbConnection dbConnection = Sqlocity.CreateDbConnection( "SqliteInMemoryDatabaseConnectionString" );

    await Sqlocity.GetDatabaseCommand( dbConnection )
        .SetCommandText( sql )
        .ExecuteNonQueryAsync( true );

    Customer customer = new Customer { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse( "06/18/1938" ) };

    // Act
    int customerId = await Sqlocity.GetDatabaseCommand( dbConnection )
        .GenerateInsertForSQLite( customer )
        .ExecuteScalarAsync<int>( true );

    // Assert
    Assert.That( customerId == 1 );
}

[Test]
public async void GenerateInsertsForSQLite_Example()
{
    // Arrange
    const string sql = @"
CREATE TABLE IF NOT EXISTS Customer
(
    CustomerId      INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
    FirstName       NVARCHAR(120)   NOT NULL,
    LastName        NVARCHAR(120)   NOT NULL,
    DateOfBirth     DATETIME        NOT NULL
);";

    DbConnection dbConnection = Sqlocity.CreateDbConnection( "SqliteInMemoryDatabaseConnectionString" );

    await Sqlocity.GetDatabaseCommand( dbConnection )
        .SetCommandText( sql )
        .ExecuteNonQueryAsync( true );

    Customer customer1 = new Customer { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse( "06/18/1938" ) };
    Customer customer2 = new Customer { FirstName = "Bruce", LastName = "Wayne", DateOfBirth = DateTime.Parse( "05/27/1939" ) };
    Customer customer3 = new Customer { FirstName = "Peter", LastName = "Parker", DateOfBirth = DateTime.Parse( "08/18/1962" ) };
    List<Customer> list = new List<Customer> { customer1, customer2, customer3 };

    // Act
    List<long> customerIds = await Sqlocity.GetDatabaseCommand( dbConnection )
        .GenerateInsertsForSQLite( list )
        .ExecuteToListAsync<long>();

    // Assert
    Assert.That( customerIds.Count == 3 );
    Assert.That( customerIds[0] == 1 );
    Assert.That( customerIds[1] == 2 );
    Assert.That( customerIds[2] == 3 );
}
    }
}