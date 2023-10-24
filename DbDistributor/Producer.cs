namespace DbDistributor;

public class Producer
{
	public static Row GenerateRow() => new() { Data = Guid.NewGuid().ToString() };
}