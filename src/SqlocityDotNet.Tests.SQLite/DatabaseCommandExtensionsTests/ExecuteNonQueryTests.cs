using System.Data;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.SQLite.DatabaseCommandExtensionsTests
{
    [TestFixture]
    public class ExecuteNonQueryTests
    {
        [Test]
        public async void Should_Return_The_Number_Of_Affected_Rows()
        {
            // Arrange
            const string sql = @"
CREATE TABLE IF NOT EXISTS SuperHero
(
    SuperHeroId     INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
    SuperHeroName	NVARCHAR(120)   NOT NULL,
    UNIQUE(SuperHeroName)
);

INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' ); /* This insert should trigger 1 row affected */
";

            // Act
            var rowsAffected = await Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( sql )
                .ExecuteNonQueryAsync();

            // Assert
            Assert.That( rowsAffected == 1 );
        }

        [Test]
        public async void Should_Null_The_DbCommand_By_Default()
        {
            // Arrange
            const string sql = @"
CREATE TABLE IF NOT EXISTS SuperHero
(
    SuperHeroId     INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
    SuperHeroName	NVARCHAR(120)   NOT NULL,
    UNIQUE(SuperHeroName)
);

INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' ); /* This insert should trigger 1 row affected */
";
            var databaseCommand = Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( sql );

            // Act
            await databaseCommand.ExecuteNonQueryAsync();

            // Assert
            Assert.IsNull( databaseCommand.DbCommand );
        }

        [Test]
        public async void Should_Keep_The_Database_Connection_Open_If_keepConnectionOpen_Parameter_Was_True()
        {
            // Arrange
            const string sql = @"
CREATE TABLE IF NOT EXISTS SuperHero
(
    SuperHeroId     INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
    SuperHeroName	NVARCHAR(120)   NOT NULL,
    UNIQUE(SuperHeroName)
);

INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' ); /* This insert should trigger 1 row affected */
";
            var databaseCommand = Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( sql );

            // Act
            var rowsAffected = await databaseCommand.ExecuteNonQueryAsync( true );

            // Assert
            Assert.That( databaseCommand.DbCommand.Connection.State == ConnectionState.Open );

            // Cleanup
            databaseCommand.Dispose();
        }

        [Test]
        public async void Should_Call_The_DatabaseCommandPreExecuteEventHandler()
        {
            // Arrange
            bool wasPreExecuteEventHandlerCalled = false;

            Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPreExecuteEventHandlers.Add( command => wasPreExecuteEventHandlerCalled = true );

            // Act
            await Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( "SELECT 1" )
                .ExecuteNonQueryAsync();

            // Assert
            Assert.IsTrue( wasPreExecuteEventHandlerCalled );
        }

        [Test]
        public async void Should_Call_The_DatabaseCommandPostExecuteEventHandler()
        {
            // Arrange
            bool wasPostExecuteEventHandlerCalled = false;

            Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPostExecuteEventHandlers.Add( command => wasPostExecuteEventHandlerCalled = true );

            // Act
            await Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( "SELECT 1" )
                .ExecuteNonQueryAsync();

            // Assert
            Assert.IsTrue( wasPostExecuteEventHandlerCalled );
        }

        [Test]
        public void Should_Call_The_DatabaseCommandUnhandledExceptionEventHandler()
        {
            // Arrange
            bool wasUnhandledExceptionEventHandlerCalled = false;

            Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandUnhandledExceptionEventHandlers.Add( ( exception, command ) =>
            {
                wasUnhandledExceptionEventHandlerCalled = true;
            } );

            // Act
            TestDelegate action = async () => await Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( "asdf;lkj" )
                .ExecuteNonQueryAsync();

            // Assert
            Assert.Throws<System.Data.SQLite.SQLiteException>( action );
            Assert.IsTrue( wasUnhandledExceptionEventHandlerCalled );
        }
    }
}