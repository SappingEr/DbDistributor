using System.Collections.Concurrent;

namespace DbDistributor;

public class Distributor
{
	private readonly int _count;
	private readonly Zone[] _zones;
	private readonly ConcurrentDictionary<int, DataBase> _dataBases = new();
	
	public Distributor(IEnumerable<Zone> zones)
	{
		if (zones is null)
			throw new ArgumentNullException(nameof(zones));
		
		_zones = zones.ToArray();
		
		foreach (var zone in _zones)
		{
			zone.DataBaseIndex = _count;
			_dataBases.TryAdd(_count, new DataBase());
			_count++;
		}
	}
	
	public IEnumerable<DataBase> DataBases => _dataBases.Values;

	public async Task DistributeAsync(Row row)
	{
		foreach (var zone in _zones)
		{
			if (zone.From <= row.ProducerId && row.ProducerId <= zone.To)
			{
				await _dataBases[zone.DataBaseIndex].AddRowAsync(row);
			}
		}
	}
}