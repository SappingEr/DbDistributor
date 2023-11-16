using System.Collections.Concurrent;

namespace DbDistributor;

public class Distributor
{
	private int count;
	private readonly Dictionary<int, DataBase> dataBases = new();
	
	public Distributor(IEnumerable<DataBase> dataBases)
	{
		if (dataBases is null)
			throw new ArgumentNullException(nameof(dataBases));

		var count = 1;

		foreach (var dataBase in dataBases)
		{
			this.dataBases.TryAdd(count, dataBase);
			count++;
		}
	}
	
	public IEnumerable<DataBase> DataBases => dataBases.Values;

	public async Task DistributeAsync(Row row)
	{
		var currentCount = Interlocked.Increment(ref count);
		
		switch (currentCount)
		{
			case <= 100:
				await dataBases[1].AddRowAsync(row);
				break;
			case <= 200:
				await dataBases[2].AddRowAsync(row);
				break;
			default:
				await dataBases[3].AddRowAsync(row);
				break;
		}
	}
}