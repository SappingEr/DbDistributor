namespace DbDistributor;

public class Distributor
{
	private readonly DataBase _dataBase;

	public Distributor(DataBase dataBase) 
		=> _dataBase = dataBase ?? throw new ArgumentNullException(nameof(dataBase));

	
	
	public Task DistributeAsync(Row row) => _dataBase.AddRowAsync(row);
}