using System.CommandLine;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DevelopsToday.CLI.ETL;

var csvFilePath = new Option<string>(
    "--csvFilePath",
    description: "Path to the CSV file."
);
var rootCommand = new RootCommand("ETL project for importing CSV data into SQL Server.")
{
    csvFilePath
};
const string connectionString =
    "Server=localhost;Database=DevelopsToday;Trusted_Connection=True;Encrypt=True;TrustServerCertificate=True;";

rootCommand.SetHandler(async (csvFilePath) =>
{
    var records = new List<EtlRecord>();
    var usedKeys = new HashSet<Key>();
    var duplicateRecords = new List<EtlRecord>();

    using var reader = new StreamReader(csvFilePath);
    using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));

    await csv.ReadAsync();
    csv.ReadHeader();
    while (await csv.ReadAsync())
    {
        var record = new EtlRecord();
        if (csv.TryGetField<DateTime?>("tpep_pickup_datetime", out var pickupDateTime))
        {
            record.TpepPickupDatetime = TimeZoneInfo.ConvertTimeToUtc(
                (DateTime)pickupDateTime!,
                TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"));
        }
        else
        {
            throw new ArgumentNullException($"TpepPickupDatetime can not be null.");
        }

        if (csv.TryGetField<DateTime?>("tpep_dropoff_datetime", out var dropoffDateTime))
        {
            record.TpepDropoffDatetime = TimeZoneInfo.ConvertTimeToUtc(
                (DateTime)dropoffDateTime!,
                TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"));
        }
        else
        {
            throw new ArgumentNullException($"TpepDropoffDatetime can not be null.");
        }

        if (csv.TryGetField<int?>("passenger_count", out var passangerCount))
        {
            record.PassengerCount = (int)passangerCount!;
        }
        else
        {
            throw new ArgumentNullException($"PassengerCount can not be null.");
        }

        record.TripDistance = csv.GetField<float?>("trip_distance");
        record.StoreAndFwdFlag = csv.GetField<string?>("store_and_fwd_flag");
        record.StoreAndFwdFlag = record.StoreAndFwdFlag == "N" ? "No" : "Yes";
        record.PULocationID = csv.GetField<int?>("PULocationID");
        record.DOLocationID = csv.GetField<int?>("DOLocationID");
        record.FareAmount = csv.GetField<float?>("fare_amount");
        record.TipAmount = csv.GetField<float?>("tip_amount");
        if (usedKeys.Contains(new Key(record.TpepPickupDatetime, record.TpepDropoffDatetime, record.PassengerCount)))
        {
            duplicateRecords.Add(record);
        }
        else
        {
            records.Add(record);
            usedKeys.Add(new Key(record.TpepPickupDatetime, record.TpepDropoffDatetime, record.PassengerCount));
        }
    }

    await using var writer =
        new StreamWriter(@"C:\Users\sobol\RiderProjects\DevelopsToday\DevelopsToday.CLI\Utils\duplicates.csv");
    await using var csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture);
    
    await csvWriter.WriteRecordsAsync(duplicateRecords);


    await using var context = new EtlDbContext(connectionString);

    await context.Database.EnsureCreatedAsync();
    await context.EtlRecords.AddRangeAsync(records);
    await context.SaveChangesAsync();


    Console.WriteLine($"Number of rows in the table: {records.Count}");
}, csvFilePath);
await rootCommand.InvokeAsync(args);

//dotnet run --project "C:\Users\sobol\RiderProjects\DevelopsToday\DevelopsToday.CLI\DevelopsToday.CLI.csproj" --csvFilePath "C:\Users\sobol\RiderProjects\DevelopsToday\DevelopsToday.CLI\Utils\sample-cab-data.csv""
record Key(DateTime? tpep_pickup_datetime, DateTime? tpep_dropoff_datetime, int? passenger_count);