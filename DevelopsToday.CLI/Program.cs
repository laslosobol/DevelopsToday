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

const string csvFilePath = "C:\\Users\\sobol\\RiderProjects\\DevelopsToday\\DevelopsToday.CLI\\Utils\\sample-cab-data.csv";
const string connectionString = "Server=localhost;Database=DevelopsToday;Trusted_Connection=True;Encrypt=True;TrustServerCertificate=True;";

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
            tpep_pickup_datetime = csv.GetField<DateTime>("tpep_pickup_datetime"),
            tpep_dropoff_datetime = csv.GetField<DateTime>("tpep_dropoff_datetime"),
            passenger_count = GetIntField(csv, "passenger_count"),
            trip_distance = csv.GetField<float>("trip_distance"),
            store_and_fwd_flag = csv.GetField<string>("store_and_fwd_flag"),
            PULocationID = GetIntField(csv, "PULocationID"),
            DOLocationID = GetIntField(csv, "DOLocationID"),
            fare_amount = csv.GetField<float>("fare_amount"),
            tip_amount = csv.GetField<float>("tip_amount")
        };

        records.Add(record);
    }
}

records.ForEach(r =>
{
    r.store_and_fwd_flag = r.store_and_fwd_flag == "N" ? "No" : "Yes";
    r.tpep_pickup_datetime = TimeZoneInfo.ConvertTimeToUtc(r.tpep_pickup_datetime, TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"));
    r.tpep_dropoff_datetime = TimeZoneInfo.ConvertTimeToUtc(r.tpep_dropoff_datetime, TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"));
});

var duplicateRecords = records.GroupBy(r => new { r.tpep_pickup_datetime, r.tpep_dropoff_datetime, r.passenger_count })
    .Where(g => g.Count() > 1)
    .SelectMany(g => g.Skip(1))
    .ToList();

using (var writer = new StreamWriter("C:\\Users\\sobol\\RiderProjects\\DevelopsToday\\DevelopsToday.CLI\\Utils\\duplicates.csv"))
using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
{
    csv.WriteRecords(duplicateRecords);
}

records = records.Except(duplicateRecords).ToList();

using (var context = new EtlDbContext(connectionString))
{
    context.Database.EnsureCreated();
    context.EtlRecords.AddRange(records);
    context.SaveChanges();
}

Console.WriteLine($"Number of rows in the table: {records.Count}");