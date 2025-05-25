using System.Collections.Concurrent;

namespace DbDistributor;

public class Distributor
{
    private readonly ConcurrentDictionary<int, DataBase> _dataBases = new();

    private readonly SortedDictionary<int, int> _hashRing = new();
    private const int VirtualNodes = 99;//Virtual baskets allow you to specify distribution intervals.


    public Distributor(IEnumerable<DataBase> dataBases)
    {
        ArgumentNullException.ThrowIfNull(dataBases);

        var id = 1;

        foreach (var db in dataBases)
        {
            _dataBases.TryAdd(id++, db);
        }

        RebuildHashRing();
    }

    public IReadOnlyCollection<DataBase> DataBases => _dataBases.Values.ToList();

    public async Task DistributeAsync(Row row)
    {
        var dataBaseId = GetDataBaseIdByConsistent(row.ProducerId);

        await _dataBases[dataBaseId]
            .AddRowAsync(new DbRow { ProducerId = row.ProducerId, Data = row.Data });
    }

    public IEnumerable<DbRow> GetProducerDataById(int producerId)
    {
        var dataBaseId = GetDataBaseIdByConsistent(producerId);
        return _dataBases[dataBaseId].Rows.Values.Where(r => r.ProducerId == producerId);
    }

    public async Task AddDatabaseAsync()
    {
        var newId = _dataBases.Count + 1;
        _dataBases.TryAdd(newId, new DataBase());

        RebuildHashRing();

        var tasks = _dataBases
                    .Where(d => d.Key != newId)
                    .Select(d => MoveRowsAsync(d.Key, d.Value));

        await Task.WhenAll(tasks);
    }

    public async Task RemoveDatabaseAsync(int databaseId)
    {
        if (_dataBases.Count <= 1)
            throw new InvalidOperationException("Cannot remove the last database.");

        if (!_dataBases.TryRemove(databaseId, out var removedDb))
            throw new KeyNotFoundException($"Database with id {databaseId} not found.");

        RebuildHashRing();

        await MoveRowsAsync(databaseId, removedDb);
    }

    private async Task MoveRowsAsync(int index, DataBase dataBase)
    {
        foreach (var kv in dataBase.Rows)
        {
            await DistributeAsync(kv.Value);
            dataBase.Rows.TryRemove(kv.Key, out _);
        }
    }

    private void RebuildHashRing() //In this algorithm, the data distribution zones of the ring can be specified. In this case, it is simply a round robin.
    {
        _hashRing.Clear();

        for (var i = 0; i < VirtualNodes; i++)
        {
            var groupIndex = i % _dataBases.Count + 1;
            _hashRing[i] = groupIndex;
        }
    }

    private int GetDataBaseIdByConsistent(int producerId)
    {
        if (_hashRing.Count == 0)
            throw new InvalidOperationException("Hash ring is empty.");

        var hash = ComputeHash(producerId);
        var tail = _hashRing.Keys.FirstOrDefault(k => k >= hash);

        var chosenKey = tail != 0 ? tail : _hashRing.Keys.First();
        return _hashRing[chosenKey];
    }

    private static int ComputeHash(int key) => key % VirtualNodes;
}