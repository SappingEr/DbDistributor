using System.Collections.Concurrent;

namespace DbDistributor;

public class DataBase
{
    public int RowCount => Rows.Count;
    public Guid Id { get; } = Guid.NewGuid();
    public ConcurrentDictionary<Guid, DbRow> Rows { get; } = [];

    public async Task AddRowAsync(Row row)
    {
        await Task.Delay(new Random().Next(50, 100));
        var dbRow = new DbRow { ProducerId = row.ProducerId, Data = row.Data };
        Rows.TryAdd(dbRow.Id, dbRow);
    }
}