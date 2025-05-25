namespace DbDistributor;

public class Distributor(DataBase dataBase)
{
    private readonly DataBase _dataBase = dataBase ?? throw new ArgumentNullException(nameof(dataBase));

    public void Distribute(Row row) => _dataBase.AddRow(row);
}