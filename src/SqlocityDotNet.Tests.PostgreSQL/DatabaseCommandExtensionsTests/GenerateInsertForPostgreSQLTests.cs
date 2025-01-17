﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.PostgreSQL.DatabaseCommandExtensionsTests
{
    [TestFixture]
    public class GenerateInsertForPostgreSQLTests
    {
        public class Customer
        {
            public int? CustomerId;
            public string FirstName;
            public string LastName;
            public DateTime DateOfBirth;
        }

        [Test]
        public async void Should_Return_The_Last_Inserted_Id()
        {
            // Arrange
            const string createSchemaSql = @"
DROP TABLE IF EXISTS Customer;

CREATE TABLE IF NOT EXISTS Customer
(
    CustomerId      serial not null,
    FirstName       VARCHAR(120)   NOT NULL,
    LastName        VARCHAR(120)   NOT NULL,
    DateOfBirth     timestamp        NOT NULL,
    PRIMARY KEY ( CustomerId )
);
";
            await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(createSchemaSql)
                .ExecuteNonQueryAsync();

            var customer = new Customer { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse("06/18/1938") };

            // Act
            var customerId = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .GenerateInsertForPostgreSQL(customer, "public.Customer")
                .ExecuteScalarAsync<int>();

            // Assert
            Assert.That(customerId == 1);
        }

        [Test]
        public async void Should_Handle_Generating_Inserts_For_A_Strongly_Typed_Object()
        {
            // Arrange
            const string createSchemaSql = @"
DROP TABLE IF EXISTS Customer;

CREATE TABLE IF NOT EXISTS Customer
(
    CustomerId      serial not null,
    FirstName       VARCHAR(120)   NOT NULL,
    LastName        VARCHAR(120)   NOT NULL,
    DateOfBirth     timestamp        NOT NULL,
    PRIMARY KEY ( CustomerId )
);
";
            await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(createSchemaSql)
                .ExecuteNonQueryAsync();

            var newCustomer = new Customer { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse("06/18/1938") };

            // Act
            var customerId = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .GenerateInsertForPostgreSQL(newCustomer)
                .ExecuteScalarAsync<int>();

            const string selectCustomerQuery = @"
SELECT  CustomerId,
		FirstName,
		LastName,
		DateOfBirth
FROM    Customer;
";

            var customer = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(selectCustomerQuery)
                .ExecuteToObjectAsync<Customer>();

            // Assert
            Assert.That(customerId == 1);
            Assert.That(customer.CustomerId == 1);
            Assert.That(customer.FirstName == newCustomer.FirstName);
            Assert.That(customer.LastName == newCustomer.LastName);
            Assert.That(customer.DateOfBirth == newCustomer.DateOfBirth);
        }

        [Test]
        public async void Should_Be_Able_To_Specify_The_Table_Name()
        {
            // Arrange
            const string createSchemaSql = @"
DROP TABLE IF EXISTS Person;

CREATE TABLE IF NOT EXISTS Person
(
    CustomerId      serial not null,
    FirstName       VARCHAR(120)   NOT NULL,
    LastName        VARCHAR(120)   NOT NULL,
    DateOfBirth     timestamp        NOT NULL,
    PRIMARY KEY ( CustomerId )
);
";
            await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(createSchemaSql)
                .ExecuteNonQueryAsync();

            var customer = new Customer { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse("06/18/1938") };

            // Act
            var customerId = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .GenerateInsertForPostgreSQL(customer, "Person") // Specifying a table name of Person
                .ExecuteScalarAsync<int>();

            // Assert
            Assert.That(customerId == 1);
        }

        [Test]
        public async void Should_Throw_An_Exception_When_Passing_An_Anonymous_Object_And_Not_Specifying_A_TableName()
        {
            // Arrange
            const string createSchemaSql = @"
DROP TABLE IF EXISTS Customer;

CREATE TABLE IF NOT EXISTS Customer
(
    CustomerId      serial not null,
    FirstName       VARCHAR(120)   NOT NULL,
    LastName        VARCHAR(120)   NOT NULL,
    DateOfBirth     timestamp        NOT NULL,
    PRIMARY KEY ( CustomerId )
);
";
            await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(createSchemaSql)
                .ExecuteNonQueryAsync();

            var customer = new { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse("06/18/1938") };

            // Act
            TestDelegate action = async () => await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .GenerateInsertForPostgreSQL(customer)
                .ExecuteScalarAsync<int>();

            // Assert
            var exception = Assert.Catch<ArgumentNullException>(action);
            Assert.That(exception.Message.Contains("The 'tableName' parameter must be provided when the object supplied is an anonymous type."));
        }

        [Test]
        public async void Should_Handle_Generating_Inserts_For_An_Anonymous_Object()
        {
            // Arrange
            const string createSchemaSql = @"
DROP TABLE IF EXISTS Customer;

CREATE TABLE IF NOT EXISTS Customer
(
    CustomerId      serial not null,
    FirstName       VARCHAR(120)   NOT NULL,
    LastName        VARCHAR(120)   NOT NULL,
    DateOfBirth     timestamp        NOT NULL,
    PRIMARY KEY ( CustomerId )
);
";
            await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(createSchemaSql)
                .ExecuteNonQueryAsync();

            var newCustomer = new { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse("06/18/1938") };

            // Act
            var customerId = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .GenerateInsertForPostgreSQL(newCustomer, "Customer")
                .ExecuteScalarAsync<int>();

            const string selectCustomerQuery = @"
SELECT  CustomerId,
		FirstName,
		LastName,
		DateOfBirth
FROM    Customer;
";

            var customer = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(selectCustomerQuery)
                .ExecuteToObjectAsync<Customer>();

            // Assert
            Assert.That(customerId == 1);
            Assert.That(customer.CustomerId == 1);
            Assert.That(customer.FirstName == newCustomer.FirstName);
            Assert.That(customer.LastName == newCustomer.LastName);
            Assert.That(customer.DateOfBirth == newCustomer.DateOfBirth);
        }

        [Test]
        public async void Should_Handle_Generating_Inserts_For_A_Dynamic_ExpandoObject()
        {
            // Arrange
            const string createSchemaSql = @"
DROP TABLE IF EXISTS Customer;

CREATE TABLE IF NOT EXISTS Customer
(
    CustomerId      serial not null,
    FirstName       VARCHAR(120)   NOT NULL,
    LastName        VARCHAR(120)   NOT NULL,
    DateOfBirth     timestamp        NOT NULL,
    PRIMARY KEY ( CustomerId )
);
";
            await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(createSchemaSql)
                .ExecuteNonQueryAsync();

            dynamic newCustomer = new ExpandoObject();
            newCustomer.FirstName = "Clark";
            newCustomer.LastName = "Kent";
            newCustomer.DateOfBirth = DateTime.Parse("06/18/1938");

            // Act
            var databaseCommand = Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString);
            databaseCommand = DatabaseCommandExtensions.GenerateInsertForPostgreSQL(databaseCommand, newCustomer, "Customer");
            var customerId = await databaseCommand
                .ExecuteScalarAsync<int>();

            const string selectCustomerQuery = @"
SELECT  CustomerId,
		FirstName,
		LastName,
		DateOfBirth
FROM    Customer;
";

            var customer = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(selectCustomerQuery)
                .ExecuteToObjectAsync<Customer>();

            // Assert
            Assert.That(customerId == 1);
            Assert.That(customer.CustomerId == 1);
            Assert.That(customer.FirstName == newCustomer.FirstName);
            Assert.That(customer.LastName == newCustomer.LastName);
            Assert.That(customer.DateOfBirth == newCustomer.DateOfBirth);
        }

        [Test]
        public async void Should_Handle_Generating_Inserts_For_A_Dictionary_Of_String_Object()
        {
            // Arrange
            const string createSchemaSql = @"
DROP TABLE IF EXISTS Customer;

CREATE TABLE IF NOT EXISTS Customer
(
    CustomerId      serial not null,
    FirstName       VARCHAR(120)   NOT NULL,
    LastName        VARCHAR(120)   NOT NULL,
    DateOfBirth     timestamp        NOT NULL,
    PRIMARY KEY ( CustomerId )
);
";
            await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(createSchemaSql)
                .ExecuteNonQueryAsync();

            dynamic newCustomer = new ExpandoObject();
            newCustomer.FirstName = "Clark";
            newCustomer.LastName = "Kent";
            newCustomer.DateOfBirth = DateTime.Parse("06/18/1938");

            // Act
            var customerId = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .GenerateInsertForPostgreSQL((IDictionary<string, object>)newCustomer, "Customer")
                .ExecuteScalarAsync<int>();

            const string selectCustomerQuery = @"
SELECT  CustomerId,
		FirstName,
		LastName,
		DateOfBirth
FROM    public.Customer;
";

            var customer = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(selectCustomerQuery)
                .ExecuteToObjectAsync<Customer>();

            // Assert
            Assert.That(customerId == 1);
            Assert.That(customer.CustomerId == 1);
            Assert.That(customer.FirstName == newCustomer.FirstName);
            Assert.That(customer.LastName == newCustomer.LastName);
            Assert.That(customer.DateOfBirth == newCustomer.DateOfBirth);
        }

        [Test]
        public async void Should_Handle_Generating_Inserts_For_An_Anonymous_Object_Converted_Into_A_Dynamic()
        {
            // Arrange
            const string createSchemaSql = @"
DROP TABLE IF EXISTS Customer;

CREATE TABLE IF NOT EXISTS Customer
(
    CustomerId      serial not null,
    FirstName       VARCHAR(120)   NOT NULL,
    LastName        VARCHAR(120)   NOT NULL,
    DateOfBirth     timestamp        NOT NULL,
    PRIMARY KEY ( CustomerId )
);
";
            await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(createSchemaSql)
                .ExecuteNonQueryAsync();

            dynamic newCustomer = new { FirstName = "Clark", LastName = "Kent", DateOfBirth = DateTime.Parse("06/18/1938") };

            // Act
            var customerId = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .GenerateInsertForPostgreSQL((object)newCustomer, "Customer")
                .ExecuteScalarAsync<int>();

            const string selectCustomerQuery = @"
SELECT  CustomerId,
		FirstName,
		LastName,
		DateOfBirth
FROM    public.Customer;
";

            var customer = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.PostgreSQLConnectionString)
                .SetCommandText(selectCustomerQuery)
                .ExecuteToObjectAsync<Customer>();

            // Assert
            Assert.That(customerId == 1);
            Assert.That(customer.CustomerId == 1);
            Assert.That(customer.FirstName == newCustomer.FirstName);
            Assert.That(customer.LastName == newCustomer.LastName);
            Assert.That(customer.DateOfBirth == newCustomer.DateOfBirth);
        }
    }
}