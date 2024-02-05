namespace DbDistributor;

// Сможет ли ГетСет написать 
public record Row
{
	public int ProducerId { get; init; }
	public string Data { get; init; } = null!;
}