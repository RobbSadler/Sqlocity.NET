using System.Data;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.SqlServer.DatabaseCommandExtensionsTests
{
    [TestFixture]
    public class ExecuteToMapTests
    {
        public class SuperHero
        {
            public long SuperHeroId;
            public string SuperHeroName;
        }

        [Test]
        public async void Should_Call_The_DataRecordCall_Action_For_Each_Record_In_The_Result_Set()
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
FROM    #SuperHero;
";

            // Act
            var superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
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

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Batman' );

SELECT  SuperHeroId,
        SuperHeroName
FROM    #SuperHero;
";
            var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( sql );

            // Act
            var superHeroes = await databaseCommand.ExecuteToMapAsync( record =>
            {
                var obj = new SuperHero
                {
                    SuperHeroId = record.GetValue( 0 ).ToLong(),
                    SuperHeroName = record.GetValue( 1 ).ToString()
                };

                return obj;
            } );

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

INSERT INTO #SuperHero ( SuperHeroName )
VALUES ( 'Batman' );

SELECT  SuperHeroId,
        SuperHeroName
FROM    #SuperHero;
";
            var databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( sql );

            // Act
            var superHeroes = await databaseCommand.ExecuteToMapAsync( record =>
            {
                var obj = new SuperHero
                {
                    SuperHeroId = record.GetValue( 0 ).ToLong(),
                    SuperHeroName = record.GetValue( 1 ).ToString()
                };

                return obj;
            }, true );

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
            var superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "SELECT 1 as SuperHeroId, 'Superman' as SuperHeroName" )
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
            Assert.IsTrue( wasPreExecuteEventHandlerCalled );
        }

        [Test]
        public async void Should_Call_The_DatabaseCommandPostExecuteEventHandler()
        {
            // Arrange
            bool wasPostExecuteEventHandlerCalled = false;

            Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPostExecuteEventHandlers.Add( command => wasPostExecuteEventHandlerCalled = true );

            // Act
            var superHeroes = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
                .SetCommandText( "SELECT 1 as SuperHeroId, 'Superman' as SuperHeroName" )
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
            Assert.Throws<System.Data.SqlClient.SqlException>( action );
            Assert.IsTrue( wasUnhandledExceptionEventHandlerCalled );
        }
    }
}