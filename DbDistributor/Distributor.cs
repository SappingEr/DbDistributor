using System.Collections.Concurrent;

namespace DbDistributor;

public class Distributor
{
    private readonly ConcurrentDictionary<int, DataBase> _dataBases = new();
    private readonly ConcurrentDictionary<int, int> _producerDataMap = new();

    public Distributor(IEnumerable<DataBase> dataBases)
    {
        ArgumentNullException.ThrowIfNull(dataBases);

        var count = 1;

        foreach (var dataBase in dataBases)
        {
            _dataBases.TryAdd(count++, dataBase);
        }
    }

    public IReadOnlyCollection<DataBase> DataBases => _dataBases.Values.ToList();

    public async Task DistributeAsync(Row row)
    {
        var dataBaseId = _producerDataMap.GetOrAdd(row.ProducerId, GetDataBaseIdByRendezvous);
        await _dataBases[dataBaseId].AddRowAsync(new DbRow { ProducerId = row.ProducerId, Data = row.Data });
    }

    public IEnumerable<DbRow> GetProducerDataById(int producerId)
    {
        var dataBaseId = _producerDataMap.GetOrAdd(producerId, GetDataBaseIdByRendezvous);
        return _dataBases[dataBaseId].Rows.Values.Where(row => row.ProducerId == producerId);
    }

    public async Task AddDatabaseAsync()
    {
        var newId = _dataBases.Count + 1;
        _dataBases.TryAdd(newId, new DataBase());

        _producerDataMap.Clear();

        var tasks = _dataBases
            .Where(d => d.Key != newId)
            .Select(async dataBase => await MoveRowsAsync(dataBase.Key, dataBase.Value));

        await Task.WhenAll(tasks);
    }

    public async Task RemoveDatabaseAsync(int databaseId)
    {
        if (_dataBases.Count <= 1)
            throw new InvalidOperationException("Cannot remove the last database.");

        if (!_dataBases.TryRemove(databaseId, out var removedDb))
            throw new KeyNotFoundException($"Database with id {databaseId} not found.");

        _producerDataMap.Clear();

        await MoveRowsAsync(databaseId, removedDb);
    }

    private int GetDataBaseIdByRendezvous(int producerId)
        => _dataBases.Keys.Select(dataBaseId => CalculateScore(producerId, dataBaseId)).MaxBy(p => p.Item2).Item1;

    private (int, int) CalculateScore(int producerId, int dataBaseId) =>
        (dataBaseId, (producerId + dataBaseId) % _dataBases.Count);

    private async Task MoveRowsAsync(int index, DataBase dataBase)
    {
        foreach (var row in dataBase.Rows)
        {
            var dataBaseId = GetDataBaseIdByRendezvous(row.Value.ProducerId);

            if (dataBaseId == index)
                continue;

            await DistributeAsync(row.Value);
            dataBase.Rows.TryRemove(row.Key, out _);
        }
    }
}