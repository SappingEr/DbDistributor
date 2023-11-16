using DbDistributor;

namespace DbDistributorTests;

public class Tests
{
	private const int RowsByUserCount = 3;
	private const int ProducersCount = 100;
	
	[Test]
	public async Task Distribute()
	{
		
		var rowsCount = RowsByUserCount * ProducersCount;
		var zones = new[]
		{
			new Zone { From = 0, To = 32 },
			new Zone { From = 33, To = 65 },
			new Zone { From = 66, To = 100 }
		};
		var distributor = new Distributor(zones);
		var producers = GetProducers(ProducersCount).ToList();
		var tasks = producers.Select(producer => AddRowsAsync(producer, distributor, RowsByUserCount)).ToList();
		await Task.WhenAll(tasks);

		var resultRowCount = distributor.DataBases.Sum(db => db.RowCount);
		var resultProducersCount = distributor.DataBases.SelectMany(db=>db.Rows)
			.GroupBy(r => r.ProducerId).Count();
		var resultRowPerProducer = distributor.DataBases.SelectMany(db=>db.Rows)
			.GroupBy(r => r.ProducerId).Average(r => r.Count());
		var resultGroupByIdCount = distributor.DataBases.SelectMany(db=>db.Rows).GroupBy(r => r.Id).Count();

		Console.WriteLine($"All rows count: {resultRowCount}");

		foreach (var dataBase in distributor.DataBases)
		{
			Console.WriteLine($"DataBase Id: {dataBase.Id}	Rows count: {dataBase.RowCount}");
			Console.WriteLine($"Db Id: {dataBase.Id}");

			foreach (var row in dataBase.Rows)
			{
				Console.WriteLine($"Producer: {row.ProducerId}	Row: {row.Id}	Data: {row.Data}");
			}
		}

		Assert.Multiple(() =>
		{
			Assert.That(resultRowCount, Is.EqualTo(rowsCount));
			Assert.That(resultProducersCount, Is.EqualTo(ProducersCount));
			Assert.That(resultRowPerProducer, Is.EqualTo(RowsByUserCount));
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