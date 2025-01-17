using System.Configuration;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.MySql.SequelocityTests
{
    [TestFixture]
    public class CreateDbConnectionTests
    {
        [Test]
        public void Should_Create_A_DbConnection_When_Supplied_A_ConnectionString_And_A_Provider_Name()
        {
            // Arrange
            string connectionString = ConfigurationManager.ConnectionStrings[ ConnectionStringsNames.MySqlConnectionString ].ConnectionString;

            const string dbProviderFactoryInvariantName = "MySql.Data.MySqlClient";

            // Act
            var dbConnection = Sqlocity.CreateDbConnection( connectionString, dbProviderFactoryInvariantName );

            // Assert
            Assert.NotNull( dbConnection );

            // Cleanup
            dbConnection.Dispose();
        }

        [Test]
        public void Should_Create_A_DbConnection_When_Supplied_A_ConnectionString_Name()
        {
            // Arrange
            string connectionString = ConnectionStringsNames.MySqlConnectionString;
            
            // Act
            var dbConnection = Sqlocity.CreateDbConnection( connectionString );

            // Assert
            Assert.NotNull( dbConnection );

            // Cleanup
            dbConnection.Dispose();
        }

        [Test]
        public void Should_Throw_A_ConnectionStringNotFoundException_When_Passed_A_Null_ConnectionString()
        {
            // Arrange
            const string connectionString = null;

            const string dbProviderFactoryInvariantName = "MySql.Data.MySqlClient";

            // Act
            TestDelegate action = () => Sqlocity.CreateDbConnection( connectionString, dbProviderFactoryInvariantName );

            // Assert
            Assert.Throws<Sqlocity.ConnectionStringNotFoundException>( action );
        }

        [Test]
        public void Should_Throw_A_DbProviderFactoryNotFoundException_When_Passed_A_Null_DbProviderFactoryInvariantName()
        {
            // Arrange
            string connectionString = ConfigurationManager.ConnectionStrings[ ConnectionStringsNames.MySqlConnectionString ].ConnectionString;

            const string dbProviderFactoryInvariantName = null;

            TestHelpers.ClearDefaultConfigurationSettings();

            // Act
            TestDelegate action = () => Sqlocity.CreateDbConnection( connectionString, dbProviderFactoryInvariantName );

            // Assert
            Assert.Throws<Sqlocity.DbProviderFactoryNotFoundException>( action );
        }

        [Test]
        public void Should_Null_The_DbCommand_On_Dispose()
        {
            // Arrange
            DatabaseCommand databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.MySqlConnectionString );

            // Act
            databaseCommand.Dispose();

            // Assert
            Assert.Null( databaseCommand.DbCommand );
        }

        [Test]
        public void Should_Null_The_DbCommand_On_Dispose_In_A_Using_Statement()
        {
            // Arrange
            DatabaseCommand databaseCommand;

            // Act
            using( databaseCommand = Sqlocity.GetDatabaseCommand( ConnectionStringsNames.MySqlConnectionString ) )
            {

            }

            // Assert
            Assert.Null( databaseCommand.DbCommand );
        }
    }
}