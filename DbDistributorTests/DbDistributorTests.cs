using DbDistributor;

namespace DbDistributorTests;

public class DbDistributorTests
{
	private const int ProducerCount = 100;
	private const int RowsPerProducer = 3;
	
	[Test]
	public void Distribute()
	{
		var producers = GetProducers(ProducerCount);
		var dataBase = new DataBase();
		var distributor = new Distributor(dataBase);
		
		foreach (var producer in producers)
		{
			for (var j = 0; j < RowsPerProducer; j++)
			{
				distributor.Distribute(producer.GenerateRow());
			}
		}

		var result = dataBase.Rows.GroupBy(r => r.Id).Count();

		Console.WriteLine($"Row count: {dataBase.RowCount}");
		Console.WriteLine($"Db Id: {dataBase.Id}");

		foreach (var row in dataBase.Rows)
		{
			Console.WriteLine($"Row: {row.Id}, Producer: {row.ProducerId}, Data: {row.Data}");
		}

		Assert.That(result, Is.EqualTo(ProducerCount * RowsPerProducer));
		Assert.That(result, Is.EqualTo(dataBase.RowCount));
	}

	private IEnumerable<Producer> GetProducers(int count)
	{
		var producers = new Producer[count];

		for (var i = 0; i < producers.Length; i++)
		{
			producers[i] = new Producer{ Id = i };
		}

		return producers;
	}
}