using System.Collections.Concurrent;

namespace DbDistributor;

public class Distributor
{
    private readonly ConcurrentDictionary<int, DataBase> _dataBases = new();

    public Distributor(IEnumerable<DataBase> dataBases)
    {
        ArgumentNullException.ThrowIfNull(dataBases);

        var count = 0;

        foreach (var dataBase in dataBases)
        {
            _dataBases.TryAdd(count++, dataBase);
        }
    }

    public IReadOnlyCollection<DataBase> DataBases => _dataBases.Values.ToList();

    public async Task DistributeAsync(Row row)
    {
        var dataBaseId = GetDataBaseId(row.ProducerId);
        await _dataBases[dataBaseId].AddRowAsync(new DbRow { ProducerId = row.ProducerId, Data = row.Data });
    }

    public IEnumerable<DbRow> GetProducerDataById(int producerId)
    {
        var dataBaseId = GetDataBaseId(producerId);
        return _dataBases[dataBaseId].Rows.Where(r => r.Value.ProducerId == producerId).Select(r => r.Value);
    }

    public async Task AddDatabaseAsync()
    {
        var newId = _dataBases.Count;
        _dataBases.TryAdd(newId, new DataBase());

        var tasks = _dataBases
            .Where(d => d.Key != newId)
            .Select(async dataBase => await MoveRowsAsync(dataBase.Key, dataBase.Value));

        await Task.WhenAll(tasks);
    }

    private int GetDataBaseId(int producerId) => producerId % _dataBases.Count;

    private async Task MoveRowsAsync(int index, DataBase dataBase)
    {
        foreach (var row in dataBase.Rows)
        {
            var dataBaseId = GetDataBaseId(row.Value.ProducerId);

            if (dataBaseId == index)
                continue;

            await DistributeAsync(row.Value);
            dataBase.Rows.TryRemove(row.Key, out _);
        }
    }
}