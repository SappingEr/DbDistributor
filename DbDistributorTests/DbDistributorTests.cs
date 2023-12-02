using DbDistributor;

namespace DbDistributorTests;

public class Tests
{
	private const int DataBaseCount = 3;
	private const int RowsByProducerCount = 3;
	private const int ProducersCount = 100;
	
	[Test]
	public async Task Distribute()
	{
		const int rowsCount = RowsByProducerCount * ProducersCount;
		var dataBases = GetDataBases(DataBaseCount);
		var distributor = new Distributor(dataBases);
		var producers = GetProducers(ProducersCount).ToList();
		var tasks = producers.Select(producer => AddRowsAsync(producer, distributor, RowsByProducerCount)).ToList();
		await Task.WhenAll(tasks);

		var resultRowCount = distributor.DataBases.Sum(db => db.RowCount);
		var resultProducersCount = distributor.DataBases.SelectMany(db=>db.Rows)
			.GroupBy(r => r.ProducerId).Count();
		var resultRowPerProducer = distributor.DataBases.SelectMany(db=>db.Rows)
			.GroupBy(r => r.ProducerId).Average(r => r.Count());
		var resultGroupByIdCount = distributor.DataBases.SelectMany(db=>db.Rows).GroupBy(r => r.Id).Count();

		WriteDataBasesData(distributor);

		Assert.Multiple(() =>
		{
			Assert.That(resultRowCount, Is.EqualTo(rowsCount));
			Assert.That(resultProducersCount, Is.EqualTo(ProducersCount));
			Assert.That(resultRowPerProducer, Is.EqualTo(RowsByProducerCount));
			Assert.That(resultGroupByIdCount, Is.EqualTo(rowsCount));
		});
	}
	
	[Test]
	public async Task RemoveDataBase()
	{
		var dataBases = GetDataBases(3);
		var distributor = new Distributor(dataBases);
		var producers = GetProducers(ProducersCount).ToList();
		var tasks = producers.Select(producer => AddRowsAsync(producer, distributor, RowsByProducerCount)).ToList();
		await Task.WhenAll(tasks);
		
		WriteDataBasesData(distributor);
		var rows = (await distributor.DestroyDataBaseAndGetRows()).ToList();
		var producerIds = rows.Select(r => r.ProducerId).Distinct();
		WriteDataBasesData(distributor);

		foreach (var producerId in producerIds)
		{
			var rowsFromOldDb = rows.Where(r=>r.ProducerId == producerId).OrderBy(r=>r.Id);
			var rowsByProducerId = distributor.GetProducerDataById(producerId).OrderBy(r=>r.Id);
		
			CollectionAssert.AreEqual(rowsFromOldDb, rowsByProducerId);
		}
	}
	
	[Test]
	public async Task AddDataBase()
	{
		var dataBases = GetDataBases(3);
		var distributor = new Distributor(dataBases);
		var producers = GetProducers(ProducersCount).ToList();
		var tasks = producers.Select(producer => AddRowsAsync(producer, distributor, RowsByProducerCount)).ToList();
		await Task.WhenAll(tasks);
		
		WriteDataBasesData(distributor);
		distributor.AddNewDataBase();
		
		foreach (var producer in producers)
		{
			var producerRows = distributor.GetProducerDataById(producer.Id).ToList();
			CollectionAssert.IsNotEmpty(producerRows);
		}
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

	private void WriteDataBasesData(Distributor distributor)
	{
		foreach (var dataBase in distributor.DataBases)
		{
			Console.WriteLine($"Db Id: {dataBase.Id}");
			Console.WriteLine($"Rows count: {dataBase.RowCount}");

			foreach (var row in dataBase.Rows)
			{
				Console.WriteLine($"Producer: {row.ProducerId}	Row: {row.Id}	Data: {row.Data}");
			}
		}

		Console.WriteLine();
	}
}