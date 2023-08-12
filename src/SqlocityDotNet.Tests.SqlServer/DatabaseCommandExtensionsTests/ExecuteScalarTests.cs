using System.Data;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.SqlServer.DatabaseCommandExtensionsTests
{
    [TestFixture]
    public class ExecuteScalarTests
    {
        [Test]
        public async void Should_Return_The_First_Column_Of_The_First_Row_In_The_Result_Set()
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
FROM    #SuperHero;
";

            // Act
            var superHeroId = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( sql )
                .ExecuteScalarAsync<long>();

            // Assert
            Assert.That( superHeroId == 1 );
        }

        [Test]
        [Description( "This tests the generic version of the ExecuteScaler method." )]
        public async void Should_Return_The_First_Column_Of_The_First_Row_In_The_Result_Set_And_Convert_It_To_The_Type_Specified()
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
FROM    #SuperHero;
";

            // Act
            var superHeroId = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( sql )
                .ExecuteScalarAsync<long>(); // Generic version of the ExecuteScalar method

            // Assert
            Assert.That( superHeroId == 1 );
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

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

SELECT  SuperHeroId, /* This should be the only value returned from ExecuteScalar */
        SuperHeroName
FROM    #SuperHero;
";
            var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
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
CREATE TABLE #SuperHero
(
    SuperHeroId     INT             NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
    SuperHeroName	NVARCHAR(120)   NOT NULL
);

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Superman' );

SELECT  SuperHeroId, /* This should be the only value returned from ExecuteScalar */
        SuperHeroName
FROM    #SuperHero;
";
            var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
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
            await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
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
            await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
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
            TestDelegate action = async () => await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "asdf;lkj" )
                .ExecuteScalarAsync();

            // Assert
            Assert.Throws<System.Data.SqlClient.SqlException>( action );
            Assert.IsTrue( wasUnhandledExceptionEventHandlerCalled );
        }
    }
}