using System.Data;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.SQLite.DatabaseCommandExtensionsTests
{
    [TestFixture]
    public class ExecuteScalarTests
    {
        [Test]
        public async void Should_Return_The_First_Column_Of_The_First_Row_In_The_Result_Set()
        {
            // Arrange
            const string sql = @"
CREATE TABLE IF NOT EXISTS SuperHero
(
    SuperHeroId     INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
    SuperHeroName	NVARCHAR(120)   NOT NULL,
    UNIQUE(SuperHeroName)
);

INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' );

SELECT  SuperHeroId, /* This should be the only value returned from ExecuteScalar */
        SuperHeroName
FROM    SuperHero;
";

            // Act
            var superHeroId = await Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( sql )
                .ExecuteScalarAsync<long>();

            // Assert
            Assert.That( superHeroId == 1 );
        }

        [Test]
        [Description("This tests the generic version of the ExecuteScaler method.")]
        public async void Should_Return_The_First_Column_Of_The_First_Row_In_The_Result_Set_And_Convert_It_To_The_Type_Specified()
        {
            // Arrange
            const string sql = @"
CREATE TABLE IF NOT EXISTS SuperHero
(
    SuperHeroId     INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
    SuperHeroName	NVARCHAR(120)   NOT NULL,
    UNIQUE(SuperHeroName)
);

INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' );

SELECT  SuperHeroId, /* This should be the only value returned from ExecuteScalar */
        SuperHeroName
FROM    SuperHero;
";

            // Act
            var superHeroId = await Sqlocity.GetDatabaseCommand(ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString)
                .SetCommandText(sql)
                .ExecuteScalarAsync<long>(); // Generic version of the ExecuteScalar method

            // Assert
            Assert.That(superHeroId == 1);
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

INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' );

SELECT  SuperHeroId, /* This should be the only value returned from ExecuteScalar */
        SuperHeroName
FROM    SuperHero;
";
            var databaseCommand = Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( sql );

            // Act
            await databaseCommand.ExecuteScalarAsync();

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

INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' );

SELECT  SuperHeroId, /* This should be the only value returned from ExecuteScalar */
        SuperHeroName
FROM    SuperHero;
";
            var databaseCommand = Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( sql );

            // Act
            await databaseCommand.ExecuteScalarAsync( true );

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
                .ExecuteScalarAsync();

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
                .ExecuteScalarAsync();

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
                .ExecuteScalarAsync();

            // Assert
            Assert.Throws<System.Data.SQLite.SQLiteException>( action );
            Assert.IsTrue( wasUnhandledExceptionEventHandlerCalled );
        }
    }
}