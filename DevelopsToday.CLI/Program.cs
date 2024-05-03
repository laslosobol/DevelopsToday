using System.CommandLine;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DevelopsToday.CLI.ETL;

int GetIntField(CsvReader csv, string fieldName)
{
    var field = csv.GetField(fieldName);
    if (string.IsNullOrWhiteSpace(field))
    {
        return 0;
    }

    return csv.GetField<int>(fieldName);
}

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

rootCommand.SetHandler((csvFilePath) =>
{
    var records = new List<EtlRecord>();

    using (var reader = new StreamReader(csvFilePath))
    using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
    {
        csv.Read();
        csv.ReadHeader();

        while (csv.Read())
        {
            var record = new EtlRecord()
            {
                tpep_pickup_datetime = csv.GetField<DateTime?>("tpep_pickup_datetime"),
                tpep_dropoff_datetime = csv.GetField<DateTime?>("tpep_dropoff_datetime"),
                passenger_count = csv.GetField<int?>("passenger_count"),
                trip_distance = csv.GetField<float?>("trip_distance"),
                store_and_fwd_flag = csv.GetField<string?>("store_and_fwd_flag"),
                PULocationID = csv.GetField<int?>("PULocationID"),
                DOLocationID = csv.GetField<int?>("DOLocationID"),
                fare_amount = csv.GetField<float?>("fare_amount"),
                tip_amount = csv.GetField<float?>("tip_amount")
            };

            records.Add(record);
        }
    }

    var usedKeys = new HashSet<Key>();
    var duplicateRecords = new List<EtlRecord>();
    var resultRecords = new List<EtlRecord>();
    
    records.ForEach(r =>
    {
        r.store_and_fwd_flag = r.store_and_fwd_flag == "N" ? "No" : "Yes";
        if (r.tpep_pickup_datetime.HasValue)
        {
            r.tpep_pickup_datetime = TimeZoneInfo.ConvertTimeToUtc((DateTime)r.tpep_pickup_datetime,
                TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"));
        }
        if (r.tpep_dropoff_datetime.HasValue)
        {
            r.tpep_dropoff_datetime = TimeZoneInfo.ConvertTimeToUtc((DateTime)r.tpep_dropoff_datetime,
                TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time")); 
        }

        if (usedKeys.Contains(new Key(r.tpep_pickup_datetime, r.tpep_dropoff_datetime, r.passenger_count)))
        {
            duplicateRecords.Add(r);
        }
        else
        {
            resultRecords.Add(r);
            usedKeys.Add(new Key(r.tpep_pickup_datetime, r.tpep_dropoff_datetime, r.passenger_count));
        }
    });
    
    using (var writer =
           new StreamWriter("C:\\Users\\sobol\\RiderProjects\\DevelopsToday\\DevelopsToday.CLI\\Utils\\duplicates.csv"))
    using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
    {
        csv.WriteRecords(duplicateRecords);
    }

    using (var context = new EtlDbContext(connectionString))
    {
        context.Database.EnsureCreated();
        context.EtlRecords.AddRange(records);
        context.SaveChanges();
    }

    Console.WriteLine($"Number of rows in the table: {records.Count}");
},csvFilePath);
rootCommand.Invoke(args);
//dotnet run --project "C:\Users\sobol\RiderProjects\DevelopsToday\DevelopsToday.CLI\DevelopsToday.CLI.csproj" --csvFilePath "C:\Users\sobol\RiderProjects\DevelopsToday\DevelopsToday.CLI\Utils\sample-cab-data.csv" --connectionString "Server=localhost;Database=DevelopsToday;Trusted_Connection=True;Encrypt=True;TrustServerCertificate=True;"
record Key(DateTime? tpep_pickup_datetime, DateTime? tpep_dropoff_datetime, int? passenger_count);