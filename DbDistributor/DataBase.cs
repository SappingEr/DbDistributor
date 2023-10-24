namespace DbDistributor;

public class DataBase
{
	public int RowCounter { get; private set; }
	public Guid Id { get; } = Guid.NewGuid();
	public List<DbRow> Rows { get; } = new();

	public void AddRow(Row row)
	{
		RowCounter++;
		Rows.Add(new DbRow { Id = RowCounter, Data = row.Data });
	}
}