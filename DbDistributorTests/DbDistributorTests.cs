using DbDistributor;

namespace DbDistributorTests;

public class Tests
{
	[Test]
	public async Task Distribute()
	{
		var rowsByUserCount = 3;
		var producersCount = 100;
		var rowsCount = rowsByUserCount * producersCount;
		var dataBase = new DataBase();
		var distributor = new Distributor(dataBase);
		var producers = GetProducers(producersCount).ToList();
		var tasks = new List<Task>();
		foreach (var producer in producers)
		{
			tasks.Add(AddRowsAsync(producer, distributor, rowsByUserCount));
		}
		await Task.WhenAll(tasks);
		
		var resultRowCount = dataBase.RowCount;
		var resultProducersCount = dataBase.Rows.GroupBy(r=>r.ProducerId).Count();
		var resultRowPerProducer = dataBase.Rows
			.GroupBy(r => r.ProducerId).Average(r=>r.Count());
		var resultGroupByIdCount = dataBase.Rows.GroupBy(r => r.Id).Count();

		Console.WriteLine($"Row count: {dataBase.RowCount}");
		Console.WriteLine($"Db Id: {dataBase.Id}");

		foreach (var row in dataBase.Rows)
		{
			Console.WriteLine($"Row {row.Id}:  Producer: {row.ProducerId}   Data: {row.Data}");
		}

        Assert.Multiple(() =>
        {
            Assert.That(resultRowCount, Is.EqualTo(rowsCount));
            Assert.That(resultProducersCount, Is.EqualTo(producersCount));
            Assert.That(resultRowPerProducer, Is.EqualTo(rowsByUserCount));
			Assert.That(resultGroupByIdCount, Is.EqualTo(rowsCount));
        });
    }

	private IEnumerable<Producer> GetProducers(int count)
	{
		var producers = new Producer[count];

		for (var i = 0; i < producers.Length; i++)
		{
			producers[i] = new Producer { Id = i };
		}
		
		return producers;
	}

	private async Task AddRowsAsync(Producer producer, Distributor distributor, int count)
	{
		for (var i = 0; i < count; i++)
		{
			await distributor.DistributeAsync(await producer.GenerateRowAsync());
		}
	}
}