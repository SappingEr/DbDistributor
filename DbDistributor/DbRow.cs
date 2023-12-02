namespace DbDistributor;

public record DbRow : Row
{
	public Guid Id { get; } = Guid.NewGuid();
}