using DbDistributor;

namespace DbDistributorTests;

public class Tests
{
	[Test]
	public void Distribute()
	{
		const int rowCount = 1000;
		var dataBase = new DataBase();
		var distributor = new Distributor(dataBase);
		for (var i = 0; i < rowCount; i++)
		{
			distributor.Distribute(Producer.GenerateRow());
		}

		var result = dataBase.Rows.GroupBy(r => r.Id).Count();

		Console.WriteLine($"Row count: {dataBase.RowCounter}");
		Console.WriteLine($"Db Id: {dataBase.Id}");

		foreach (var row in dataBase.Rows)
		{
			Console.WriteLine($"Row {row.Id}: {row.Data}");
		}

		Assert.That(result, Is.EqualTo(rowCount));
		Assert.That(result, Is.EqualTo(rowCount));
	}
}