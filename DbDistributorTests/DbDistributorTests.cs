using DbDistributor;

namespace DbDistributorTests;

[TestFixture]
public class Tests
{
    private const int DataBaseCount = 3;
    private const int RowsByUserCount = 3;
    private const int ProducersCount = 100;
    private const int RowsCount = RowsByUserCount * ProducersCount;

    [Test]
    public async Task Distribute()
    {
        var dataBases = GetDataBases(DataBaseCount);
        var distributor = new Distributor(dataBases);
        var producers = GetProducers(ProducersCount).ToList();
        var tasks = producers.Select(producer => AddRowsAsync(producer, distributor, RowsByUserCount)).ToList();
        await Task.WhenAll(tasks);

        var resultRowCount = distributor.DataBases.Sum(db => db.RowCount);
        var resultProducersCount = distributor.DataBases.SelectMany(db => db.Rows)
            .GroupBy(r => r.Value.ProducerId).Count();
        var resultRowPerProducer = distributor.DataBases.SelectMany(db => db.Rows)
            .GroupBy(r => r.Value.ProducerId).Average(r => r.Count());
        var resultGroupByIdCount = distributor.DataBases.SelectMany(db => db.Rows).GroupBy(r => r.Key).Count();

        WriteDataBasesDataToConsole(distributor);

        Assert.Multiple(() =>
        {
            Assert.That(resultRowCount, Is.EqualTo(RowsCount));
            Assert.That(resultProducersCount, Is.EqualTo(ProducersCount));
            Assert.That(resultRowPerProducer, Is.EqualTo(RowsByUserCount));
            Assert.That(resultGroupByIdCount, Is.EqualTo(RowsCount));
        });
    }

    [Test]
    public async Task DistributeAndAdd()
    {
        var dataBases = GetDataBases(DataBaseCount);
        var distributor = new Distributor(dataBases);
        var producers = GetProducers(ProducersCount).ToList();
        var tasks = producers.Select(producer => AddRowsAsync(producer, distributor, RowsByUserCount)).ToList();
        await Task.WhenAll(tasks);
        await distributor.AddDatabaseAsync();

        var resultRowCount = distributor.DataBases.Sum(db => db.RowCount);
        var resultProducersCount = distributor.DataBases.SelectMany(db => db.Rows)
            .GroupBy(r => r.Value.ProducerId).Count();
        var resultRowPerProducer = distributor.DataBases.SelectMany(db => db.Rows)
            .GroupBy(r => r.Value.ProducerId).Average(r => r.Count());
        var resultGroupByIdCount = distributor.DataBases.SelectMany(db => db.Rows).GroupBy(r => r.Key).Count();

        WriteDataBasesDataToConsole(distributor);

        Assert.Multiple(() =>
        {
            Assert.That(resultRowCount, Is.EqualTo(RowsCount));
            Assert.That(resultProducersCount, Is.EqualTo(ProducersCount));
            Assert.That(resultRowPerProducer, Is.EqualTo(RowsByUserCount));
            Assert.That(resultGroupByIdCount, Is.EqualTo(RowsCount));
        });
    }

    private static IEnumerable<DataBase> GetDataBases(int count)
    {
        var dataBases = new DataBase[count];

        for (var i = 0; i < count; i++)
        {
            dataBases[i] = new DataBase();
        }

        return dataBases;
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

    private void WriteDataBasesDataToConsole(Distributor distributor)
    {
        foreach (var dataBase in distributor.DataBases)
        {
            Console.WriteLine($"Db Id: {dataBase.Id}");
            Console.WriteLine($"Rows count: {dataBase.RowCount}");

            foreach (var row in dataBase.Rows)
            {
                Console.WriteLine($"Producer: {row.Value.ProducerId}	Row: {row.Key}	Data: {row.Value.Data}");
            }
        }

        Console.WriteLine();
    }
}