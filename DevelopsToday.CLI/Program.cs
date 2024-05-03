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
    var usedKeys = new HashSet<Key>();
    var duplicateRecords = new List<EtlRecord>();

    using (var reader = new StreamReader(csvFilePath))
    using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
    {
        csv.Read();
        csv.ReadHeader();
        while (csv.Read())
        {
            var record = new EtlRecord();
            if (csv.GetField<DateTime?>("tpep_pickup_datetime").HasValue)
            {
                record.TpepPickupDatetime = TimeZoneInfo.ConvertTimeToUtc((DateTime)csv.GetField<DateTime?>("tpep_pickup_datetime")!,
                    TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"));
            }
            else
            {
                throw new ArgumentNullException($"TpepPickupDatetime can not be null.");
            }
            if (csv.GetField<DateTime?>("tpep_dropoff_datetime").HasValue)
            {
                record.TpepDropoffDatetime = TimeZoneInfo.ConvertTimeToUtc((DateTime)csv.GetField<DateTime?>("tpep_dropoff_datetime")!,
                    TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time")); 
            }
            else
            {
                throw new ArgumentNullException($"TpepDropoffDatetime can not be null.");
            }
            if (csv.GetField<int?>("passenger_count").HasValue)
            {
                record.PassengerCount = (int)csv.GetField<int?>("passenger_count")!;
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
            records.Add(record);
        }
    }
    
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
//dotnet run --project "C:\Users\sobol\RiderProjects\DevelopsToday\DevelopsToday.CLI\DevelopsToday.CLI.csproj" --csvFilePath "C:\Users\sobol\RiderProjects\DevelopsToday\DevelopsToday.CLI\Utils\sample-cab-data.csv""
record Key(DateTime? tpep_pickup_datetime, DateTime? tpep_dropoff_datetime, int? passenger_count);