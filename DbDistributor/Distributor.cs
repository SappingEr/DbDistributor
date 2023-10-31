namespace DbDistributor;

public class Distributor
{
	private readonly DataBase _dataBase;

	public Distributor(DataBase dataBase)
		=> _dataBase = dataBase ?? throw new ArgumentNullException(nameof(dataBase));

	public async Task DistributeAsync(Row row) => await _dataBase.AddRowAsync(row);
}