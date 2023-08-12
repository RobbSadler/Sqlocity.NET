using System.Transactions;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.SqlServer.DatabaseCommandExtensionsTests
{
	[TestFixture]
	public class BeginTransactionTests
	{
		[Test]
		public async void Should_Return_A_New_DbTransaction()
		{
			// Arrange
            var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString );

			// Act
			var transaction = databaseCommand.BeginTransaction();
			
			// Assert
			Assert.NotNull( transaction );
			Assert.That( databaseCommand.DbCommand.Connection == transaction.Connection );
		}

		[Test]
		public async void Should_Associate_The_DbTransaction_With_The_DatabaseCommand()
		{
			// Arrange
			var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString );

			// Act
			var transaction = databaseCommand.BeginTransaction();

			// Assert
			Assert.NotNull( databaseCommand.DbCommand.Transaction == transaction );
		}

		[Test]
		public async void Can_Rollback_Transaction()
		{
			const string createTableSchema = @"
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
		        .SetCommandText( createTableSchema )
		        .ExecuteNonQueryAsync();

            var rowCount = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                .ExecuteScalarAsync<int>();

            Assert.That( rowCount == 0 );

			const string sqlCommand1 = @"
INSERT INTO Customer VALUES ( 'Clark', 'Kent', '06/18/1938' );
INSERT INTO Customer VALUES ( 'Bruce', 'Wayne', '05/27/1939' );
";

			const string sqlCommand2 = @"
INSERT INTO Customer VALUES ( 'Peter', 'Parker', '08/18/1962' );
";

			using( var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString ) )
			{
				using( var transaction = databaseCommand.BeginTransaction() )
				{
					var rowsUpdated = await databaseCommand
                        .SetCommandText( sqlCommand1 )
						.ExecuteNonQueryAsync( keepConnectionOpen: true );

					var nextRowsUpdated = await databaseCommand
                        .SetCommandText( sqlCommand2 )
						.ExecuteNonQueryAsync( keepConnectionOpen: true );

					Assert.That( rowsUpdated == 2 && nextRowsUpdated == 1 );

                    rowCount = await databaseCommand
                        .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                        .ExecuteScalarAsync<int>( keepConnectionOpen: true );

                    Assert.That( rowCount == 3 );

					if( rowsUpdated == 2 && nextRowsUpdated == 1 )
						transaction.Rollback();
				}
			}

            rowCount = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                .ExecuteScalarAsync<int>();

            Assert.That( rowCount == 0 );
		}

        [Test]
        public async void Can_Commit_Transactions()
        {
            const string createTableSchema = @"
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
                .SetCommandText( createTableSchema )
                .ExecuteNonQueryAsync();

            var rowCount = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                .ExecuteScalarAsync<int>();

            Assert.That( rowCount == 0 );

            const string sqlCommand1 = @"
INSERT INTO Customer VALUES ( 'Clark', 'Kent', '06/18/1938' );
INSERT INTO Customer VALUES ( 'Bruce', 'Wayne', '05/27/1939' );
";

            const string sqlCommand2 = @"
INSERT INTO Customer VALUES ( 'Peter', 'Parker', '08/18/1962' );
";

            using( var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString ) )
            {
                using( var transaction = databaseCommand.BeginTransaction() )
                {
                    var rowsUpdated = await databaseCommand
                        .SetCommandText( sqlCommand1 )
                        .ExecuteNonQueryAsync( keepConnectionOpen: true );

                    var nextRowsUpdated = await databaseCommand
                        .SetCommandText( sqlCommand2 )
                        .ExecuteNonQueryAsync( keepConnectionOpen: true );

                    Assert.That( rowsUpdated == 2 && nextRowsUpdated == 1 );

                    rowCount = await databaseCommand
                        .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                        .ExecuteScalarAsync<int>( keepConnectionOpen: true );

                    Assert.That( rowCount == 3 );

                    if( rowsUpdated == 2 && nextRowsUpdated == 1 )
                        transaction.Commit();
                }
            }

            rowCount = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                .ExecuteScalarAsync<int>();

            Assert.That( rowCount == 3 );
        }

        [Test]
        public async void Can_Rollback_Transaction_Using_TransactionScope()
        {
            const string createTableSchema = @"
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
                .SetCommandText( createTableSchema )
                .ExecuteNonQueryAsync();

            var rowCount = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                .ExecuteScalarAsync<int>();

            Assert.That( rowCount == 0 );

            const string sqlCommand1 = @"
INSERT INTO Customer VALUES ( 'Clark', 'Kent', '06/18/1938' );
INSERT INTO Customer VALUES ( 'Bruce', 'Wayne', '05/27/1939' );
";

            const string sqlCommand2 = @"
INSERT INTO Customer VALUES ( 'Peter', 'Parker', '08/18/1962' );
";

            using( var transaction = new TransactionScope() )
            {
                var rowsUpdated = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                        .SetCommandText( sqlCommand1 )
                        .ExecuteNonQueryAsync();

                var nextRowsUpdated = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                    .SetCommandText( sqlCommand2 )
                    .ExecuteNonQueryAsync();

                Assert.That( rowsUpdated == 2 && nextRowsUpdated == 1 );

                rowCount = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                        .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                        .ExecuteScalarAsync<int>();

                Assert.That( rowCount == 3 );

                if ( rowsUpdated == 2 && nextRowsUpdated == 1 )
                    transaction.Dispose();
            }

            rowCount = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                .ExecuteScalarAsync<int>();

            Assert.That( rowCount == 0 );
        }

        [Test]
        public async void Can_Commit_Transactions_Using_TransactionScope()
        {
            const string createTableSchema = @"
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
                .SetCommandText( createTableSchema )
                .ExecuteNonQueryAsync();

            var rowCount = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                .ExecuteScalarAsync<int>();

            Assert.That( rowCount == 0 );

            const string sqlCommand1 = @"
INSERT INTO Customer VALUES ( 'Clark', 'Kent', '06/18/1938' );
INSERT INTO Customer VALUES ( 'Bruce', 'Wayne', '05/27/1939' );
";

            const string sqlCommand2 = @"
INSERT INTO Customer VALUES ( 'Peter', 'Parker', '08/18/1962' );
";

            using( var transaction = new TransactionScope() )
            {
                var rowsUpdated = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                        .SetCommandText( sqlCommand1 )
                        .ExecuteNonQueryAsync();

                var nextRowsUpdated = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                    .SetCommandText( sqlCommand2 )
                    .ExecuteNonQueryAsync();

                Assert.That( rowsUpdated == 2 && nextRowsUpdated == 1 );

                rowCount = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                        .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                        .ExecuteScalarAsync<int>();

                Assert.That( rowCount == 3 );

                if( rowsUpdated == 2 && nextRowsUpdated == 1 )
                    transaction.Complete();
            }

            rowCount = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "SELECT COUNT(*) FROM Customer" )
                .ExecuteScalarAsync<int>();

            Assert.That( rowCount == 3 );
        }
	}
}