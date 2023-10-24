namespace DbDistributor;

public class Producer
{
	public int Id { get; init; }
	
	public Row GenerateRow()
	{
		var random = new Random();
		Thread.Sleep(random.Next(100, 300));
		return new Row { ProducerId = Id, Data = Guid.NewGuid().ToString() };
	}
}