using System.Collections.Concurrent;

namespace DbDistributor;

public class Distributor
{
	private readonly ConcurrentDictionary<int, DataBase> _dataBases = new();
	private readonly ConcurrentDictionary<int, int> _producerDataMap = new();

	public Distributor(IEnumerable<DataBase> dataBases)
	{
		if (dataBases is null)
		{
			throw new ArgumentNullException(nameof(dataBases));
		}

		var count = 1;

		foreach (var dataBase in dataBases)
		{
			_dataBases.TryAdd(count, dataBase);
			count++;
		}
	}

	public IEnumerable<DataBase> DataBases => _dataBases.Values;


	public async Task DistributeAsync(Row row)
	{
		var dataBaseId = 0;

		if (_producerDataMap.TryGetValue(row.ProducerId, out var existingDataBaseId))
		{
			dataBaseId = existingDataBaseId;
		}
		else
		{
			dataBaseId = GetDataBaseIdByRendezvous(row.ProducerId);
			_producerDataMap.TryAdd(row.ProducerId, dataBaseId);
		}

		await _dataBases[dataBaseId].AddRowAsync(new DbRow { ProducerId = row.ProducerId, Data = row.Data });
	}

	public void AddNewDataBase()
	{
		var newKey = _dataBases.Count + 1;
		_dataBases.TryAdd(newKey, new DataBase());
	}

	public async Task DestroyDataBase()
	{
		var key = _dataBases.Keys.Max();
		var rows = _dataBases[key].Rows.ToList();
		var mapKeysToRemove = _producerDataMap.Where(p => p.Value == key).Select(p => p.Key).ToList();
		
		foreach (var mapKey in mapKeysToRemove)
		{
			_producerDataMap.TryRemove(mapKey, out _);
		}

		if (_dataBases.TryRemove(key, out _))
		{
			var tasks = rows.Select(async r => { await DistributeAsync(r); }).ToList();
			await Task.WhenAll(tasks);
		}
	}

	public IEnumerable<DbRow> GetProducerDataById(int producerId)
	{
		if (_producerDataMap.TryGetValue(producerId, out var eXdataBaseId))
		{
			return _dataBases[eXdataBaseId].Rows.Where(r => r.ProducerId == producerId);
		}
			 
		return Enumerable.Empty<DbRow>();
	}

	private int GetDataBaseIdByRendezvous(int producerId)
	{
		return _dataBases.Keys.Select(dataBaseId => CalculateScore(producerId, dataBaseId)).MaxBy(p => p.Item2).Item1;
	}

	private (int, int) CalculateScore(int producerId, int dataBaseId)
	{
		return (dataBaseId, (producerId + dataBaseId) % _dataBases.Count);
	}
}