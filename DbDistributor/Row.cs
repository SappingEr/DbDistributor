namespace DbDistributor;

public record Row
{
	public int ProducerId { get; init; }
	public string Data { get; init; } = null!;
}