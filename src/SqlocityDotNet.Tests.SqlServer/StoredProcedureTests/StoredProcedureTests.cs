using System.Data;
using NUnit.Framework;

namespace SqlocityNetCore.Tests.SqlServer.StoredProcedureTests
{
	[TestFixture]
	public class StoredProcedureTests
	{
		public class SuperHero
		{
			public long SuperHeroId;
			public string SuperHeroName;
		}

		[Test]
		public async void Stored_Procedure_Test_Using_SetCommandType()
		{
			// Arrange
			const string dropStoredProcedureSql = @"
IF OBJECT_ID('GetSuperHeroByName', 'P') IS NOT NULL
	DROP PROC GetSuperHeroByName
";

			const string createStoredProcedureSql = @"
CREATE PROCEDURE GetSuperHeroByName @SuperHeroName VARCHAR(120)
AS
	BEGIN

		CREATE TABLE #SuperHero
		(
			SuperHeroId	INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
			SuperHeroName VARCHAR(120) NOT NULL
		);

		INSERT INTO #SuperHero ( SuperHeroName ) VALUES  ( 'Superman' );
		INSERT INTO #SuperHero ( SuperHeroName ) VALUES  ( 'Batman' );
		INSERT INTO #SuperHero ( SuperHeroName ) VALUES  ( 'Spider-Man' );

		SELECT	*
		FROM	#SuperHero sh
		WHERE	sh.SuperHeroName = @SuperHeroName

	END
";

            await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
				.SetCommandText( dropStoredProcedureSql )
				.ExecuteNonQueryAsync();

            await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
				.SetCommandText( createStoredProcedureSql )
				.ExecuteNonQueryAsync();

			// Act
			var superhero = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
				.SetCommandType( CommandType.StoredProcedure )
				.SetCommandText( "GetSuperHeroByName" )
				.AddParameter( "@SuperHeroName", "Superman", DbType.AnsiString )
				.ExecuteToObjectAsync<SuperHero>();

			// Assert
			Assert.NotNull( superhero );
			Assert.That( superhero.SuperHeroName == "Superman" );
		}

		[Test]
		public async void Stored_Procedure_Test_Using_Exec_Statement()
		{
			// Arrange
			const string dropStoredProcedureSql = @"
IF OBJECT_ID('GetSuperHeroByName', 'P') IS NOT NULL
	DROP PROC GetSuperHeroByName
";

			const string createStoredProcedureSql = @"
CREATE PROCEDURE GetSuperHeroByName @SuperHeroName VARCHAR(120)
AS
	BEGIN

		CREATE TABLE #SuperHero
		(
			SuperHeroId	INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
			SuperHeroName VARCHAR(120) NOT NULL
		);

		INSERT INTO #SuperHero ( SuperHeroName ) VALUES  ( 'Superman' );
		INSERT INTO #SuperHero ( SuperHeroName ) VALUES  ( 'Batman' );
		INSERT INTO #SuperHero ( SuperHeroName ) VALUES  ( 'Spider-Man' );

		SELECT	*
		FROM	#SuperHero sh
		WHERE	sh.SuperHeroName = @SuperHeroName

	END
";

            await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
				.SetCommandText( dropStoredProcedureSql )
				.ExecuteNonQueryAsync();

            await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
				.SetCommandText( createStoredProcedureSql )
				.ExecuteNonQueryAsync();

			// Act
			var superhero = await Sqlocity.GetDatabaseCommand( ConnectionStringsNames.SqlServerConnectionString )
				.SetCommandText( "EXEC GetSuperHeroByName @SuperHeroName" )
				.AddParameter( "@SuperHeroName", "Superman", DbType.AnsiString )
				.ExecuteToObjectAsync<SuperHero>();

			// Assert
			Assert.NotNull( superhero );
			Assert.That( superhero.SuperHeroName == "Superman" );
		}
	}
}