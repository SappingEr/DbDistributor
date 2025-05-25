namespace DbDistributor;

public class Distributor
{
    private readonly Zone[] _zones;
    private readonly DataBase[] _dataBases;

    public Distributor(IEnumerable<Zone> zones)
    {
        ArgumentNullException.ThrowIfNull(zones);

        _zones = zones.OrderBy(z => z.From).ToArray();

        for (var i = 1; i < _zones.Length; i++)
        {
            if (_zones[i].From <= _zones[i - 1].To)
            {
                throw new ArgumentException();
            }
        }

        _dataBases = new DataBase[_zones.Length];

        for (var index = 0; index < _zones.Length; index++)
        {
            _dataBases[index] = new DataBase();
        }
    }

    public IEnumerable<DataBase> DataBases => _dataBases;

    public async Task DistributeAsync(Row row)
    {
        ArgumentNullException.ThrowIfNull(row);

        var index = FindZoneIndex(row.ProducerId);

        switch (index)
        {
            case >= 0:
                await _dataBases[index].AddRowAsync(row);
                break;
            default:
                throw new InvalidOperationException($"No zone found for ProducerId {row.ProducerId}");
        }
    }

    private int FindZoneIndex(int producerId)
    {
        var left = 0;
        var right = _zones.Length - 1;

        while (left <= right)
        {
            var mid = left + (right - left) / 2;
            var zone = _zones[mid];

            if (producerId < zone.From)
                right = mid - 1;
            else if (producerId > zone.To)
                left = mid + 1;
            else
                return mid;
        }

        return -1;
    }
}