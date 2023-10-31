namespace DbDistributor;

public class Producer
{
	public int Id { get; init; }
	
	
	public async Task<Row> GenerateRowAsync()
	{
		await Task.Delay(new Random().Next(10, 50));
		return new Row { ProducerId = Id, Data = Guid.NewGuid().ToString() };
	}
}