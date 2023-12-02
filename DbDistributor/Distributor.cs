using System.Collections.Concurrent;

namespace DbDistributor;

public class Distributor
{
	private readonly ConcurrentDictionary<int, DataBase> _dataBases = new();

	public Distributor(IEnumerable<DataBase> dataBases)
	{
		if (dataBases is null)
			throw new ArgumentNullException(nameof(dataBases));

		var count = 0;

		foreach (var dataBase in dataBases)
		{
			_dataBases.TryAdd(count, dataBase);
			count++;
		}
	}

	public IEnumerable<DataBase> DataBases => _dataBases.Values;

	public void AddNewDataBase()
	{
		var newKey = _dataBases.Keys.Max() + 1;
		_dataBases.TryAdd(newKey, new DataBase());
	}
	
	public async Task<IEnumerable<DbRow>> DestroyDataBaseAndGetRows()
	{
		var key = _dataBases.Keys.Max();
		var rows = _dataBases[key].Rows.ToList();
		
		if (_dataBases.TryRemove(key, out _))
		{
			var tasks = rows.Select(async r  =>
			{
				await _dataBases[GetDataBaseIdByRendezvous(r.ProducerId)].AddRowAsync(r);
			}).ToList();
			await Task.WhenAll(tasks);
			return rows;
		}

		return Enumerable.Empty<DbRow>();
	}

	public async Task DistributeAsync(Row row)
	{
		var dataBaseId = GetDataBaseIdByRendezvous(row.ProducerId);
		await _dataBases[dataBaseId].AddRowAsync(new DbRow{ProducerId = row.ProducerId, Data = row.Data});
	}
	
	public IEnumerable<DbRow> GetProducerDataById(int producerId)
	{
		var dataBaseId = GetDataBaseIdByRendezvous(producerId);
		return _dataBases[dataBaseId].Rows.Where(r=>r.ProducerId == producerId);
	}

	private int GetDataBaseIdByRendezvous(int producerId)
		=> _dataBases.Keys.Select(dataBaseId => CalculateScore(producerId, dataBaseId)).MaxBy(p => p.Item2).Item1;

	private (int, int) CalculateScore(int producerId, int dataBaseId)
		=> (dataBaseId, (producerId + dataBaseId) % _dataBases.Count);
}

