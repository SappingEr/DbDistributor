using DbDistributor;

namespace DbDistributorTests;

[TestFixture]
public class Tests
{
    private const int RowsByUserCount = 3;
    private const int ProducersCount = 100;

    [Test]
    public async Task Distribute()
    {
        const int rowsCount = RowsByUserCount * ProducersCount;
        var dataBase = new DataBase();
        var distributor = new Distributor(dataBase);
        var producers = GetProducers(ProducersCount).ToList();
        var tasks = producers.Select(producer => AddRowsAsync(producer, distributor, RowsByUserCount)).ToList();

        await Task.WhenAll(tasks);

        var resultRowCount = dataBase.RowCount;
        var resultProducersCount = dataBase.Rows.GroupBy(r => r.ProducerId).Count();
        var resultRowPerProducer = dataBase.Rows
                                           .GroupBy(r => r.ProducerId)
                                           .Average(r => r.Count());
        var resultGroupByIdCount = dataBase.Rows.GroupBy(r => r.Id).Count();

        Console.WriteLine($"Row count: {dataBase.RowCount}");
        Console.WriteLine($"Db Id: {dataBase.Id}");

        foreach (var row in dataBase.Rows)
        {
            Console.WriteLine($"ProducerId: {row.ProducerId}	Row: {row.Id}	Data: {row.Data}");
        }

        Assert.Multiple(() =>
        {
            Assert.That(resultRowCount, Is.EqualTo(rowsCount));
            Assert.That(resultProducersCount, Is.EqualTo(ProducersCount));
            Assert.That(resultRowPerProducer, Is.EqualTo(RowsByUserCount));
            Assert.That(resultGroupByIdCount, Is.EqualTo(rowsCount));
        });
    }

    private static IEnumerable<Producer> GetProducers(int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new Producer { Id = i };
        }
    }

    private static async Task AddRowsAsync(Producer producer, Distributor distributor, int count)
    {
        for (var i = 0; i < count; i++)
        {
            await distributor.DistributeAsync(await producer.GenerateRowAsync());
        }
    }
}