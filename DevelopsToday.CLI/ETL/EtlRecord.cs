namespace DevelopsToday.CLI.ETL;

public class EtlRecord
{
    public int Id { get; set; }
    public DateTime? tpep_pickup_datetime { get; set; }
    public DateTime? tpep_dropoff_datetime { get; set; }
    public int? passenger_count { get; set; }
    public float? trip_distance { get; set; }
    public string? store_and_fwd_flag { get; set; }
    public int? PULocationID { get; set; }
    public int? DOLocationID { get; set; }
    public float? fare_amount { get; set; }
    public float? tip_amount { get; set; }
}