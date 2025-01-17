using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.SQLite.DatabaseCommandExtensionsTests
{
    [TestFixture]
    public class ExecuteReaderTests
    {
        [Test]
        public async void Should_Call_The_DataRecordCall_Action_For_Each_Record_In_The_Result_Set()
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
INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Batman' );

SELECT  SuperHeroId,
        SuperHeroName
FROM    SuperHero;
";

            var list = new List<object>();

            // Act
            await Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
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
INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Batman' );

SELECT  SuperHeroId,
        SuperHeroName
FROM    SuperHero;
";
            var databaseCommand = Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( sql );

            var list = new List<object>();

            // Act
            await databaseCommand.ExecuteReaderAsync( record =>
            {
                var obj = new
                {
                    SuperHeroId = record.GetValue( 0 ),
                    SuperHeroName = record.GetValue( 1 )
                };

                list.Add( obj );
            } );

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
INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Batman' );

SELECT  SuperHeroId,
        SuperHeroName
FROM    SuperHero;
";
            var databaseCommand = Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( sql );

            var list = new List<object>();

            // Act
            await databaseCommand.ExecuteReaderAsync( record =>
            {
                var obj = new
                {
                    SuperHeroId = record.GetValue( 0 ),
                    SuperHeroName = record.GetValue( 1 )
                };

                list.Add( obj );
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
            await Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
                .SetCommandText( "SELECT 1" )
                .ExecuteReaderAsync( record => { } );

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
                .ExecuteReaderAsync( record => { } );

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
                .ExecuteReaderAsync( record => { } );

            // Assert
            Assert.Throws<System.Data.SQLite.SQLiteException>( action );
            Assert.IsTrue( wasUnhandledExceptionEventHandlerCalled );
        }
    }

//    [TestFixture]
//    public class ExecuteReader_Of_Type_T_Tests
//    {
//        [Test]
//        public async void Should_Call_The_DataRecordCall_Func_For_Each_Record_In_The_Result_Set()
//        {
//            // Arrange
//            const string sql = @"
//CREATE TABLE IF NOT EXISTS SuperHero
//(
//    SuperHeroId     INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
//    SuperHeroName	NVARCHAR(120)   NOT NULL,
//    UNIQUE(SuperHeroName)
//);

//INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' );
//INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Batman' );

//SELECT  SuperHeroId,
//        SuperHeroName
//FROM    SuperHero;
//";

//            List<object> list;

//            // Act
//            list = await Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
//                .SetCommandText( sql )
//                .ExecuteReaderAsync<object>( record => new
//                {
//                    SuperHeroId = record.GetValue( 0 ),
//                    SuperHeroName = record.GetValue( 1 )
//                } )
//                .ToList();


//            // Assert
//            Assert.That( list.Count == 2 );
//        }

//        [Test]
//        public async void Should_Null_The_DbCommand_By_Default()
//        {
//            // Arrange
//            const string sql = @"
//CREATE TABLE IF NOT EXISTS SuperHero
//(
//    SuperHeroId     INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
//    SuperHeroName	NVARCHAR(120)   NOT NULL,
//    UNIQUE(SuperHeroName)
//);

//INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' );
//INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Batman' );

//SELECT  SuperHeroId,
//        SuperHeroName
//FROM    SuperHero;
//";
//            var databaseCommand = Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
//                .SetCommandText( sql );

//            List<object> list;

//            // Act
//            list = databaseCommand
//                .ExecuteReader<object>( record => new
//                {
//                    SuperHeroId = record.GetValue( 0 ),
//                    SuperHeroName = record.GetValue( 1 )
//                } )
//                .ToList();

//            // Assert
//            Assert.IsNull( databaseCommand.DbCommand );
//        }

//        [Test]
//        public async void Should_Keep_The_Database_Connection_Open_If_keepConnectionOpen_Parameter_Was_True()
//        {
//            // Arrange
//            const string sql = @"
//CREATE TABLE IF NOT EXISTS SuperHero
//(
//    SuperHeroId     INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
//    SuperHeroName	NVARCHAR(120)   NOT NULL,
//    UNIQUE(SuperHeroName)
//);

//INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' );
//INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Batman' );

//SELECT  SuperHeroId,
//        SuperHeroName
//FROM    SuperHero;
//";
//            var databaseCommand = Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
//                .SetCommandText( sql );

//            List<object> list;

//            // Act
//            list = databaseCommand
//                .ExecuteReader<object>( record => new
//                {
//                    SuperHeroId = record.GetValue( 0 ),
//                    SuperHeroName = record.GetValue( 1 )
//                }, true )
//                .ToList();

//            // Assert
//            Assert.That( databaseCommand.DbCommand.Connection.State == ConnectionState.Open );

//            // Cleanup
//            databaseCommand.Dispose();
//        }

        //[Test]
        //public async void Should_Call_The_DatabaseCommandPreExecuteEventHandler()
        //{
        //    // Arrange
        //    bool wasPreExecuteEventHandlerCalled = false;

        //    Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPreExecuteEventHandlers.Add( command => wasPreExecuteEventHandlerCalled = true );

        //    // Act
        //    Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
        //        .SetCommandText( "SELECT 1" )
        //        .ExecuteReader<object>( record => new { } )
        //        .ToList();

        //    // Assert
        //    Assert.IsTrue( wasPreExecuteEventHandlerCalled );
        //}

//        [Test]
//        public async void Should_Call_The_DatabaseCommandPostExecuteEventHandler()
//        {
//            // Arrange
//            bool wasPostExecuteEventHandlerCalled = false;

//            Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandPostExecuteEventHandlers.Add( command => wasPostExecuteEventHandlerCalled = true );

//            // Act
//            Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
//                .SetCommandText( "SELECT 1" )
//                .ExecuteReader<object>( record => new { } )
//                .ToList();

//            // Assert
//            Assert.IsTrue( wasPostExecuteEventHandlerCalled );
//        }

//        [Test]
//        public async void Should_Call_The_DatabaseCommandUnhandledExceptionEventHandler()
//        {
//            // Arrange
//            bool wasUnhandledExceptionEventHandlerCalled = false;

//            Sqlocity.ConfigurationSettings.EventHandlers.DatabaseCommandUnhandledExceptionEventHandlers.Add( ( exception, command ) =>
//            {
//                wasUnhandledExceptionEventHandlerCalled = true;
//            } );

//            // Act
//            TestDelegate action = () => Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
//                .SetCommandText( "asdf;lkj" )
//                .ExecuteReader<object>( record => new { } )
//                .ToList();

//            // Assert
//            Assert.Throws<System.Data.SQLite.SQLiteException>( action );
//            Assert.IsTrue( wasUnhandledExceptionEventHandlerCalled );
//        }

//        [Test]
//        public async void Should_Null_The_DbCommand_If_Iteration_Ends_Before_Full_Enumeration()
//        {
//            // Arrange
//            const string sql = @"
//CREATE TABLE IF NOT EXISTS SuperHero
//(
//    SuperHeroId     INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
//    SuperHeroName	NVARCHAR(120)   NOT NULL,
//    UNIQUE(SuperHeroName)
//);

//INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' );
//INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Batman' );

//SELECT  SuperHeroId,
//        SuperHeroName
//FROM    SuperHero;
//";
//            var databaseCommand = Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
//                .SetCommandText( sql );

//            // Act
//            databaseCommand
//                .ExecuteReader( record => new
//                {
//                    SuperHeroId = record.GetValue( 0 ),
//                    SuperHeroName = record.GetValue( 1 )
//                } )
//                .First();

//            // Assert
//            Assert.IsNull( databaseCommand.DbCommand );
//        }

//        [Test]
//        public async void Should_Null_The_DbCommand_If_Exception_Occurs_During_Iteration()
//        {
//            // Arrange
//            const string sql = @"
//CREATE TABLE IF NOT EXISTS SuperHero
//(
//    SuperHeroId     INTEGER         NOT NULL    PRIMARY KEY     AUTOINCREMENT,
//    SuperHeroName	NVARCHAR(120)   NOT NULL,
//    UNIQUE(SuperHeroName)
//);

//INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Superman' );
//INSERT OR IGNORE INTO SuperHero VALUES ( NULL, 'Batman' );

//SELECT  SuperHeroId,
//        SuperHeroName
//FROM    SuperHero;
//";
//            var databaseCommand = Sqlocity.GetDatabaseCommandForSQLite( ConnectionStringsNames.SqliteInMemoryDatabaseConnectionString )
//                .SetCommandText( sql );

//            var iter = databaseCommand.ExecuteReader( record => new
//            {
//                SuperHeroId = record.GetValue( 0 ),
//                SuperHeroName = record.GetValue( 1 )
//            } );

//            // Act
//            try
//            {
//                foreach ( var item in iter )
//                {
//                    throw new Exception( "Exception occured during iteration." );
//                }
//            }
//            catch { }

//            // Assert
//            Assert.IsNull( databaseCommand.DbCommand );
//        }
//    }
}