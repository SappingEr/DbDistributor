namespace DbDistributor;

public class Distributor
{
	private readonly Dictionary<int, DataBase> _dataBases = new();
	
	public Distributor(IEnumerable<DataBase> dataBases)
	{
		if (dataBases is null)
			throw new ArgumentNullException(nameof(dataBases));

		var count = 1;

		foreach (var dataBase in dataBases)
		{
			_dataBases.Add(count, dataBase);
			count++;
		}
	}


	public async Task DistributeAsync(Row row)
	{
		var dbId = row.ProducerId % _dataBases.Count;
		var dataBase = _dataBases[dbId];
		await dataBase.AddRowAsync(row);
	}
}