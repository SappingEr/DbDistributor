namespace DbDistributor;

public class DataBase
{
	public int RowCount => Rows.Count;
	public Guid Id { get; } = Guid.NewGuid();
	public List<DbRow> Rows { get; } = new();

	public void AddRow(Row row)
	{
		var random = new Random();
		Thread.Sleep(random.Next(50, 100));
		Rows.Add(new DbRow { ProducerId = row.ProducerId, Data = row.Data });
	}
}