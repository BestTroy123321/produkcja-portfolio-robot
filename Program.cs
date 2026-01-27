using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;

namespace SubiektConnector
{
    public class PozycjaDoZmiany
    {
        public int DokId { get; set; }
        public string NumerPelny { get; set; }
        public string SymbolTowaru { get; set; }
        public string NazwaTowaru { get; set; }
        public decimal StaraCena { get; set; }
        public DateTime DataWystawienia { get; set; }
    }

    public class DokumentPayload
    {
        [JsonProperty("dok_id")]
        public int DokId { get; set; }

        [JsonProperty("dok_nr_pelny")]
        public string DokNrPelny { get; set; }

        [JsonProperty("dok_data_wyst")]
        public string DokDataWyst { get; set; }

        [JsonProperty("pozycje")]
        public List<PozycjaPayload> Pozycje { get; set; }
    }

    public class PozycjaPayload
    {
        [JsonProperty("symbol")]
        public string Symbol { get; set; }

        [JsonProperty("nazwa")]
        public string Nazwa { get; set; }

        [JsonProperty("cena_oryginalna_netto")]
        public decimal CenaOryginalnaNetto { get; set; }
    }

    internal class Program
    {
        private const string N8N_URL = "https://example.com/webhook/test";

        private static void Main(string[] args)
        {
            var connectionString = "Server=SERVER_NAME;Database=DATABASE_NAME;User Id=USER_NAME;Password=PASSWORD;";

            Console.WriteLine("Łączenie z bazą...");

            try
            {
                var pozycje = new List<PozycjaDoZmiany>();
                var dokumenty = new Dictionary<int, DokumentPayload>();

                using (var connection = new SqlConnection(connectionString))
                using (var command = new SqlCommand(GetSqlQuery(), connection))
                {
                    connection.Open();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var pozycja = new PozycjaDoZmiany
                            {
                                DokId = reader.GetInt32(0),
                                SymbolTowaru = reader.GetString(1),
                                StaraCena = reader.GetDecimal(2),
                                NumerPelny = reader.GetString(3),
                                NazwaTowaru = reader.GetString(4),
                                DataWystawienia = reader.GetDateTime(5)
                            };

                            pozycje.Add(pozycja);
                            if (!dokumenty.TryGetValue(pozycja.DokId, out var dokument))
                            {
                                dokument = new DokumentPayload
                                {
                                    DokId = pozycja.DokId,
                                    DokNrPelny = pozycja.NumerPelny,
                                    DokDataWyst = pozycja.DataWystawienia.ToString("dd.MM.yyyy"),
                                    Pozycje = new List<PozycjaPayload>()
                                };
                                dokumenty.Add(pozycja.DokId, dokument);
                            }

                            dokument.Pozycje.Add(new PozycjaPayload
                            {
                                Symbol = pozycja.SymbolTowaru,
                                Nazwa = pozycja.NazwaTowaru,
                                CenaOryginalnaNetto = pozycja.StaraCena
                            });
                        }
                    }
                }

                Console.WriteLine("Znaleziono " + dokumenty.Count + " dokumentów.");

                var payload = JsonConvert.SerializeObject(new List<DokumentPayload>(dokumenty.Values));
                Console.WriteLine("Wysyłanie do n8n...");

                using (var httpClient = new HttpClient())
                using (var content = new StringContent(payload, Encoding.UTF8, "application/json"))
                {
                    var response = httpClient.PostAsync(N8N_URL, content).GetAwaiter().GetResult();
                    var responseBody = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

                    Console.WriteLine("Odpowiedź serwera: " + (int)response.StatusCode + " " + response.ReasonPhrase);
                    if (!string.IsNullOrWhiteSpace(responseBody))
                    {
                        Console.WriteLine("Treść odpowiedzi: " + responseBody);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd: " + ex.Message);
            }
        }

        private static string GetSqlQuery()
        {
            return @"
SELECT 
    d.dok_Id,
    t.tw_Symbol,
    p.ob_CenaNetto AS cena_na_dokumencie,
    d.dok_NrPelny,
    t.tw_Nazwa,
    d.dok_DataWyst
FROM dok__Dokument d 
JOIN dok_Pozycja p ON p.ob_DokHanId = d.dok_Id 
JOIN tw__Towar t ON p.ob_TowId = t.tw_Id 
JOIN sl_GrupaTw g ON t.tw_IdGrupa = g.grt_Id 
WHERE 
    d.dok_Typ = 15
    AND d.dok_Status IN (5, 6, 7) 
    AND d.dok_DataWyst >= DATEADD(hour, -48, GETDATE()) 
    AND g.grt_Nazwa = 'KONFEKCJA' 
ORDER BY d.dok_DataWyst DESC";
        }
    }
}
