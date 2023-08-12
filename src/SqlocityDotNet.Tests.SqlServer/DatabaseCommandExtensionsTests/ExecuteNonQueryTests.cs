using System.Data;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.SqlServer.DatabaseCommandExtensionsTests
{
    [TestFixture]
    public class ExecuteNonQueryTests
    {
        [Test]
        public async void Should_Return_The_Number_Of_Affected_Rows()
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
VALUES ( 'Superman' );
";

            // Act
            var rowsAffected = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
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
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

/* This insert should trigger 1 row affected */
INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );
";
            var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
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
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

/* This insert should trigger 1 row affected */
INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );
";
            var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( sql );

            // Act
            var rowsAffected = await databaseCommand.ExecuteNonQueryAsync(true);

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
            await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
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
            await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
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
            TestDelegate action = async () => await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "asdf;lkj" )
                .ExecuteNonQueryAsync();

            // Assert
            Assert.Throws<System.Data.SqlClient.SqlException>( action );
            Assert.IsTrue( wasUnhandledExceptionEventHandlerCalled );
        }
    }
}