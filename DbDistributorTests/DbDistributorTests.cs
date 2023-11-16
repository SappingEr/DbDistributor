using DbDistributor;

namespace DbDistributorTests;

public class Tests
{
	[Test]
	public async Task Distribute()
	{
		const int rowsByUserCount = 3;
		const int producersCount = 100;
		var rowsCount = rowsByUserCount * producersCount;
		var dataBases = GetDataBases(3);
		var distributor = new Distributor(dataBases);
		var producers = GetProducers(producersCount).ToList();
		var tasks = new List<Task>();
		
		foreach (var producer in producers)
		{
			tasks.Add(AddRowsAsync(producer, distributor, rowsByUserCount));
		}

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

	private IEnumerable<DataBase> GetDataBases(int count)
	{
		var dataBases = new DataBase[count];
		
		for (int i = 0; i < count; i++)
		{
			dataBases[i] = new DataBase();
		}
		
		return dataBases;
	}


	private async Task AddRowsAsync(Producer producer, Distributor distributor, int count)
	{
		for (var i = 0; i < count; i++)
		{
			await distributor.DistributeAsync(await producer.GenerateRowAsync());
		}
	}
}