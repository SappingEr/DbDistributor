namespace DbDistributor;

public class Distributor(DataBase dataBase)
{
    public async Task DistributeAsync(Row row) => await dataBase.AddRowAsync(row);
}