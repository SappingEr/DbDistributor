using System.Collections.Concurrent;

namespace DbDistributor;

public class DataBase
{
	public int RowCount => Rows.Count;
	public Guid Id { get; } = Guid.NewGuid();
	public ConcurrentBag<DbRow> Rows { get; } = new();


	public async Task AddRowAsync(DbRow row)
	{
		await Task.Delay(new Random().Next(50, 100));
		Rows.Add(row);
	}
}