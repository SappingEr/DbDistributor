using System.Collections.Concurrent;

namespace DbDistributor;

public class DataBase
{
	private int _rowCount;

	public int RowCount => _rowCount;
	public Guid Id { get; } = Guid.NewGuid();
	public ConcurrentBag<DbRow> Rows { get; } = new();

	
	public async Task AddRowAsync(Row row)
	{
		await Task.Delay(new Random().Next(1000, 3000));
		var num = Interlocked.Increment(ref _rowCount);
		Rows.Add(new DbRow { Id = num, ProducerId = row.ProducerId, Data = row.Data });
	}
}