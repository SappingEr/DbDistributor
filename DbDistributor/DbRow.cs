namespace DbDistributor;

public class DbRow : Row
{
	public Guid Id { get; } = Guid.NewGuid();
}