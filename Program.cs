using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Runtime.InteropServices;
using System.Threading;
using System.Reflection;
using Newtonsoft.Json;

namespace SubiektConnector
{
    public class DokumentZmianyDto
    {
        [JsonProperty("dok_id")]
        public int DokId { get; set; }

        [JsonProperty("dok_nr_pelny")]
        public string DokNrPelny { get; set; }

        [JsonProperty("pozycje")]
        public List<PozycjaZmianyDto> Pozycje { get; set; }
    }

    public class PozycjaZmianyDto
    {
        [JsonProperty("symbol")]
        public string Symbol { get; set; }

        [JsonProperty("nowa_cena")]
        public decimal? NowaCena { get; set; }
    }

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

    public class FzPozycjaPayload
    {
        [JsonProperty("symbol")]
        public string Symbol { get; set; }

        [JsonProperty("ilosc")]
        public decimal Ilosc { get; set; }
    }

    public class FzContextPayload
    {
        [JsonProperty("fz_id")]
        public int FzId { get; set; }

        [JsonProperty("fz_numer")]
        public string FzNumer { get; set; }
    }

    public class FzPayload
    {
        [JsonProperty("context")]
        public FzContextPayload Context { get; set; }

        [JsonProperty("produkty")]
        public List<FzPozycjaPayload> Produkty { get; set; }
    }

    public class FzWebhookResponseDto
    {
        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("context")]
        public FzWebhookContextDto Context { get; set; }

        [JsonProperty("dane_do_rw")]
        public FzDaneDoRwDto DaneDoRw { get; set; }
    }

    public class FzWebhookContextDto
    {
        [JsonProperty("fz_id")]
        public int FzId { get; set; }

        [JsonProperty("fz_numer")]
        public string FzNumer { get; set; }
    }

    public class FzDaneDoRwDto
    {
        [JsonProperty("opis")]
        public string Opis { get; set; }

        [JsonProperty("pozycje")]
        public List<FzRwPozycjaDto> Pozycje { get; set; }
    }

    public class FzRwPozycjaDto
    {
        [JsonProperty("symbol_surowca")]
        public string SymbolSurowca { get; set; }

        [JsonProperty("ilosc_laczna")]
        public string IloscLaczna { get; set; }

        [JsonProperty("jednostka")]
        public string Jednostka { get; set; }
    }

    internal class Program
    {
        private static LoggerService _logger;
        private static Timer _heartbeatTimer;

        [STAThread]
        private static void Main(string[] args)
        {
            _logger = new LoggerService();
            _logger.AddLog("INFO", "Uruchomienie robota");

            var intervalMinutes = ParseIntSetting(ConfigurationManager.AppSettings["IntervalMinutes"], 30);
            if (intervalMinutes < 1)
            {
                intervalMinutes = 30;
            }

            StartHeartbeat();
            RunLoop(intervalMinutes);
        }

        private static void RunLoop(int intervalMinutes)
        {
            dynamic subiekt = null;

            while (true)
            {
                try
                {
                    if (subiekt == null)
                    {
                        subiekt = ZalogujSubiektGT();
                        if (subiekt == null)
                        {
                            _logger.AddLog("ERROR", "Logowanie do Sfery nie powiodło się", new { stackTrace = Environment.StackTrace });
                            _logger.FlushAsync().GetAwaiter().GetResult();
                            Thread.Sleep(TimeSpan.FromMinutes(intervalMinutes));
                            continue;
                        }
                    }

                    ExecuteOnce(subiekt);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Błąd: " + ex.Message);
                    _logger.AddLog("ERROR", ex.Message, new { stackTrace = ex.ToString() });
                    try { ZakonczSubiekt(subiekt); } catch { }
                    try { Marshal.ReleaseComObject(subiekt); } catch { }
                    subiekt = null;
                }
                finally
                {
                    try
                    {
                        _logger.FlushAsync().GetAwaiter().GetResult();
                    }
                    catch
                    {
                    }
                }

                Thread.Sleep(TimeSpan.FromMinutes(intervalMinutes));
            }
        }

        private static void ExecuteOnce(dynamic subiekt)
        {
            var connectionString = ConfigurationManager.ConnectionStrings["SubiektDB"]?.ConnectionString;
            var zdWebhookUrl = ConfigurationManager.AppSettings["ZdWebhookUrl"];

            Console.WriteLine("Etap 1: Poprawa ZD");
            _logger.AddLog("INFO", "Etap 1: rozpoczęto poprawę ZD");

            Console.WriteLine("Łączenie z bazą...");

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                Console.WriteLine("Brak connection stringa: SubiektDB");
                _logger.AddLog("ERROR", "Brak connection stringa: SubiektDB", new { stackTrace = Environment.StackTrace });
                return;
            }

            if (string.IsNullOrWhiteSpace(zdWebhookUrl))
            {
                Console.WriteLine("Brak ustawienia appSettings: ZdWebhookUrl");
                _logger.AddLog("ERROR", "Brak ustawienia appSettings: ZdWebhookUrl", new { stackTrace = Environment.StackTrace });
                _logger.AddLog("INFO", "Etap 1: pominięto poprawę ZD, przechodzę do etapu 2");
                Console.WriteLine("Etap 1: Pominięto poprawę ZD, przechodzę do etapu 2.");
                _logger.AddLog("INFO", "Etap 2: rozpoczęto tworzenie RW na podstawie FZ");
                Console.WriteLine("Etap 2: Tworzenie RW na podstawie FZ");
                ExecuteFzWebhook(connectionString, subiekt);
                return;
            }

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
            _logger.AddLog("SUCCESS", "Pobrano " + pozycje.Count + " pozycji w " + dokumenty.Count + " dokumentach", new { liczbaDokumentow = dokumenty.Count, liczbaPozycji = pozycje.Count });

            var payload = JsonConvert.SerializeObject(new List<DokumentPayload>(dokumenty.Values));
            Console.WriteLine("Wysyłanie do n8n...");

            using (var httpClient = new HttpClient())
            using (var content = new StringContent(payload, Encoding.UTF8, "application/json"))
            {
                var response = httpClient.PostAsync(zdWebhookUrl, content).GetAwaiter().GetResult();
                var responseBody = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

                Console.WriteLine("Odpowiedź serwera: " + (int)response.StatusCode + " " + response.ReasonPhrase);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    Console.WriteLine("Etap 1: Webhook zwrócił błędny status, pomijam poprawę ZD.");
                    _logger.AddLog("ERROR", "Etap 1: webhook zwrócił błędny status", new { stackTrace = responseBody });
                }
                else if (string.IsNullOrWhiteSpace(responseBody))
                {
                    Console.WriteLine("Etap 1: Webhook nie zwrócił treści, pomijam poprawę ZD.");
                    _logger.AddLog("INFO", "Etap 1: webhook nie zwrócił treści");
                }
                else
                {
                    Console.WriteLine("Treść odpowiedzi: " + responseBody);
                    if (!CzyPoprawnaOdpowiedzWebhooka(responseBody))
                    {
                        Console.WriteLine("Etap 1: Webhook nie zwrócił wymaganych danych, pomijam poprawę ZD.");
                        _logger.AddLog("ERROR", "Etap 1: webhook nie zwrócił wymaganych danych", new { stackTrace = responseBody });
                    }
                    else
                    {
                        var result = PrzetworzDokumenty(responseBody, subiekt);
                        if (result != null)
                        {
                            _logger.AddLog("SUCCESS", "Podsumowanie aktualizacji", new { zaktualizowano = result.Zaktualizowano, bledy = result.Bledy });
                        }
                    }
                }
            }

            _logger.AddLog("INFO", "Etap 2: rozpoczęto tworzenie RW na podstawie FZ");
            Console.WriteLine("Etap 2: Tworzenie RW na podstawie FZ");
            ExecuteFzWebhook(connectionString, subiekt);
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
    AND d.dok_DataWyst >= DATEADD(hour, -168, GETDATE()) 
    AND g.grt_Nazwa = 'KONFEKCJA' 
ORDER BY d.dok_DataWyst DESC";
        }

        private static string GetFzSqlQuery()
        {
            return @"
SELECT 
    d.dok_Id, 
    d.dok_NrPelny AS Numer_FZ, 
    d.dok_DataWyst, 
    t.tw_Nazwa, 
    t.tw_Symbol, 
    p.ob_Ilosc 
FROM dok__Dokument d 
JOIN dok_Pozycja p ON d.dok_Id = p.ob_DokHanId 
JOIN tw__Towar t ON p.ob_TowId = t.tw_Id 
WHERE 
    d.dok_Typ = 1
    AND t.tw_IdGrupa = 263
    AND d.dok_DataWyst >= DATEADD(day, -3, GETDATE())
    AND NOT EXISTS ( 
        SELECT 1 
        FROM dok__Dokument rw 
        WHERE rw.dok_Typ = 13 
        AND rw.dok_Uwagi LIKE '%AUTO-RW: ' + d.dok_NrPelny + '%' 
    ) 
ORDER BY d.dok_DataWyst DESC";
        }

        private static void ExecuteFzWebhook(string connectionString, dynamic subiekt)
        {
            var fzWebhookUrl = ConfigurationManager.AppSettings["FzWebhookUrl"];
            if (string.IsNullOrWhiteSpace(fzWebhookUrl))
            {
                _logger.AddLog("ERROR", "Brak ustawienia appSettings: FzWebhookUrl", new { stackTrace = Environment.StackTrace });
                return;
            }

            var dokumenty = new Dictionary<int, FzPayload>();
            var liczbaPozycji = 0;

            try
            {
                using (var connection = new SqlConnection(connectionString))
                using (var command = new SqlCommand(GetFzSqlQuery(), connection))
                {
                    connection.Open();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var fzId = reader.GetInt32(0);
                            var fzNumer = reader.GetString(1);
                            var symbol = reader.GetString(4);
                            var ilosc = reader.GetDecimal(5);

                            if (!dokumenty.TryGetValue(fzId, out var dokument))
                            {
                                dokument = new FzPayload
                                {
                                    Context = new FzContextPayload
                                    {
                                        FzId = fzId,
                                        FzNumer = fzNumer
                                    },
                                    Produkty = new List<FzPozycjaPayload>()
                                };
                                dokumenty.Add(fzId, dokument);
                            }

                            dokument.Produkty.Add(new FzPozycjaPayload
                            {
                                Symbol = symbol,
                                Ilosc = ilosc
                            });
                            liczbaPozycji++;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: Błąd podczas pobierania FZ: " + ex.Message);
                _logger.AddLog("ERROR", "Etap 2: błąd pobierania FZ", new { stackTrace = ex.ToString() });
                return;
            }

            if (dokumenty.Count == 0)
            {
                Console.WriteLine("Etap 2: Brak FZ do przetworzenia.");
                _logger.AddLog("INFO", "Etap 2: brak FZ do przetworzenia");
                return;
            }

            Console.WriteLine("Etap 2: Znaleziono " + dokumenty.Count + " FZ z " + liczbaPozycji + " produktami.");
            _logger.AddLog("SUCCESS", "Etap 2: znaleziono " + dokumenty.Count + " FZ z " + liczbaPozycji + " produktami", new { liczbaFz = dokumenty.Count, liczbaPozycji = liczbaPozycji });

            var payload = JsonConvert.SerializeObject(new List<FzPayload>(dokumenty.Values));
            string responseBody = null;
            try
            {
                using (var httpClient = new HttpClient())
                using (var content = new StringContent(payload, Encoding.UTF8, "application/json"))
                {
                    var response = httpClient.PostAsync(fzWebhookUrl, content).GetAwaiter().GetResult();
                    responseBody = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        Console.WriteLine("Etap 2: Webhook zwrócił błędny status.");
                        _logger.AddLog("ERROR", "Etap 2: webhook zwrócił błędny status", new { stackTrace = responseBody });
                        return;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: Błąd podczas wysyłki webhooka: " + ex.Message);
                _logger.AddLog("ERROR", "Etap 2: błąd wysyłki webhooka", new { stackTrace = ex.ToString() });
                return;
            }

            _logger.AddLog("INFO", "Etap 2: rozpoczęto tworzenie RW z odpowiedzi webhooka");
            Console.WriteLine("Etap 2: Tworzenie RW z odpowiedzi webhooka");
            PrzetworzRwZWebhooka(responseBody, subiekt);
        }

        private static void PrzetworzRwZWebhooka(string responseBody, dynamic subiekt)
        {
            if (string.IsNullOrWhiteSpace(responseBody))
            {
                Console.WriteLine("Etap 2: Webhook nie zwrócił treści, pomijam RW.");
                _logger.AddLog("INFO", "Etap 2: webhook nie zwrócił treści, pominięto RW");
                return;
            }

            List<FzWebhookResponseDto> odpowiedzi;
            try
            {
                odpowiedzi = JsonConvert.DeserializeObject<List<FzWebhookResponseDto>>(responseBody) ?? new List<FzWebhookResponseDto>();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: Nie udało się zdeserializować odpowiedzi webhooka.");
                _logger.AddLog("ERROR", "Etap 2: błąd deserializacji odpowiedzi webhooka", new { stackTrace = ex.ToString() });
                return;
            }

            if (odpowiedzi.Count == 0)
            {
                Console.WriteLine("Etap 2: Brak danych do RW.");
                _logger.AddLog("INFO", "Etap 2: brak danych do RW");
                return;
            }

            var utworzono = 0;
            var bledy = 0;

            foreach (var odpowiedz in odpowiedzi)
            {
                try
                {
                    Console.WriteLine("Etap 2: Start RW dla FZ " + odpowiedz?.Context?.FzNumer);
                    _logger.AddLog("INFO", "Etap 2: start RW", new { fzId = odpowiedz?.Context?.FzId, fzNumer = odpowiedz?.Context?.FzNumer });
                    if (odpowiedz == null || !string.Equals(odpowiedz.Status, "success", StringComparison.OrdinalIgnoreCase))
                    {
                        bledy++;
                        Console.WriteLine("Etap 2: Status różny od success dla FZ " + (odpowiedz?.Context?.FzNumer ?? "brak numeru"));
                        _logger.AddLog("ERROR", "Etap 2: webhook zwrócił status inny niż success", new { fzId = odpowiedz?.Context?.FzId, fzNumer = odpowiedz?.Context?.FzNumer });
                        continue;
                    }

                    if (odpowiedz.DaneDoRw == null || odpowiedz.DaneDoRw.Pozycje == null || odpowiedz.DaneDoRw.Pozycje.Count == 0)
                    {
                        bledy++;
                        Console.WriteLine("Etap 2: Brak pozycji do RW dla FZ " + odpowiedz.Context?.FzNumer);
                        _logger.AddLog("ERROR", "Etap 2: brak pozycji do RW", new { fzId = odpowiedz.Context?.FzId, fzNumer = odpowiedz.Context?.FzNumer });
                        continue;
                    }

                    dynamic rw = subiekt.Dokumenty.Dodaj(13);
                    Console.WriteLine("Etap 2: Utworzono obiekt RW dla FZ " + odpowiedz.Context?.FzNumer);
                    _logger.AddLog("INFO", "Etap 2: utworzono obiekt RW", new { fzId = odpowiedz.Context?.FzId, fzNumer = odpowiedz.Context?.FzNumer });
                    if (!string.IsNullOrWhiteSpace(odpowiedz.DaneDoRw.Opis))
                    {
                        rw.Uwagi = odpowiedz.DaneDoRw.Opis;
                        Console.WriteLine("Etap 2: Ustawiono uwagi RW dla FZ " + odpowiedz.Context?.FzNumer + ": " + odpowiedz.DaneDoRw.Opis);
                        _logger.AddLog("INFO", "Etap 2: ustawiono uwagi RW", new { fzId = odpowiedz.Context?.FzId, fzNumer = odpowiedz.Context?.FzNumer, opis = odpowiedz.DaneDoRw.Opis });
                    }
                    UstawMagazynDlaRw(rw, subiekt, odpowiedz.Context?.FzNumer);

                    var dodanePozycje = 0;
                    foreach (var pozycja in odpowiedz.DaneDoRw.Pozycje)
                    {
                        if (!TryParseIlosc(pozycja.IloscLaczna, out var ilosc))
                        {
                            bledy++;
                            Console.WriteLine("Etap 2: Niepoprawna ilość dla FZ " + odpowiedz.Context?.FzNumer + ", symbol: " + pozycja.SymbolSurowca + ", ilość: " + pozycja.IloscLaczna);
                            _logger.AddLog("ERROR", "Etap 2: niepoprawna ilość", new { symbol = pozycja.SymbolSurowca, ilosc = pozycja.IloscLaczna, fzId = odpowiedz.Context?.FzId, fzNumer = odpowiedz.Context?.FzNumer });
                            continue;
                        }

                        if (ilosc <= 0)
                        {
                            Console.WriteLine("Etap 2: Pominięto pozycję z ilością <= 0 dla FZ " + odpowiedz.Context?.FzNumer + ", symbol: " + pozycja.SymbolSurowca);
                            _logger.AddLog("ERROR", "Etap 2: pominięto pozycję z ilością <= 0", new { symbol = pozycja.SymbolSurowca, ilosc = ilosc, fzId = odpowiedz.Context?.FzId, fzNumer = odpowiedz.Context?.FzNumer });
                            continue;
                        }

                        var towar = PobierzTowarPoSymbolu(subiekt, pozycja.SymbolSurowca);
                        if (towar == null)
                        {
                            bledy++;
                            Console.WriteLine("Etap 2: Nie znaleziono towaru " + pozycja.SymbolSurowca + " dla FZ " + odpowiedz.Context?.FzNumer);
                            _logger.AddLog("ERROR", "Etap 2: nie znaleziono towaru", new { symbol = pozycja.SymbolSurowca, fzId = odpowiedz.Context?.FzId, fzNumer = odpowiedz.Context?.FzNumer });
                            continue;
                        }

                        var towarId = PobierzWartoscInt(towar, "Id");
                        if (towarId <= 0)
                        {
                            bledy++;
                            Console.WriteLine("Etap 2: Nie udało się ustalić Id towaru " + pozycja.SymbolSurowca + " dla FZ " + odpowiedz.Context?.FzNumer);
                            _logger.AddLog("ERROR", "Etap 2: brak Id towaru", new { symbol = pozycja.SymbolSurowca, fzId = odpowiedz.Context?.FzId, fzNumer = odpowiedz.Context?.FzNumer });
                            continue;
                        }

                        dynamic poz = rw.Pozycje.Dodaj(towarId);
                        UstawIloscPozycji(poz, ilosc, pozycja.SymbolSurowca, odpowiedz.Context?.FzNumer);
                        dodanePozycje++;
                        Console.WriteLine("Etap 2: Dodano pozycję RW dla FZ " + odpowiedz.Context?.FzNumer + ", symbol: " + pozycja.SymbolSurowca + ", ilość: " + ilosc);
                        _logger.AddLog("INFO", "Etap 2: dodano pozycję RW", new { symbol = pozycja.SymbolSurowca, ilosc = ilosc, fzId = odpowiedz.Context?.FzId, fzNumer = odpowiedz.Context?.FzNumer });
                    }

                    if (dodanePozycje == 0)
                    {
                        bledy++;
                        Console.WriteLine("Etap 2: RW bez pozycji dla FZ " + odpowiedz.Context?.FzNumer + ", pomijam zapis.");
                        _logger.AddLog("ERROR", "Etap 2: RW bez pozycji, pominięto zapis", new { fzId = odpowiedz.Context?.FzId, fzNumer = odpowiedz.Context?.FzNumer });
                        try { rw.Zamknij(); } catch { }
                        continue;
                    }

                    Console.WriteLine("Etap 2: Zapisuję RW dla FZ " + odpowiedz.Context?.FzNumer);
                    rw.Zapisz();
                    Console.WriteLine("Etap 2: Zamykam RW dla FZ " + odpowiedz.Context?.FzNumer);
                    rw.Zamknij();
                    utworzono++;
                    Console.WriteLine("Etap 2: RW zapisane poprawnie dla FZ " + odpowiedz.Context?.FzNumer);
                    _logger.AddLog("SUCCESS", "Etap 2: RW zapisane poprawnie", new { fzId = odpowiedz.Context?.FzId, fzNumer = odpowiedz.Context?.FzNumer, liczbaPozycji = dodanePozycje });
                }
                catch (Exception ex)
                {
                    bledy++;
                    Console.WriteLine("Etap 2: Błąd tworzenia RW dla FZ " + odpowiedz?.Context?.FzNumer + ": " + ex.Message);
                    _logger.AddLog("ERROR", "Etap 2: błąd tworzenia RW", new { fzId = odpowiedz?.Context?.FzId, fzNumer = odpowiedz?.Context?.FzNumer, step = "tworzenie_zapis_zamkniecie", stackTrace = ex.ToString() });
                }
            }

            _logger.AddLog("SUCCESS", "Etap 2: utworzono RW", new { utworzono = utworzono, bledy = bledy });
            Console.WriteLine("Etap 2: Utworzono RW: " + utworzono + ", błędy: " + bledy);
        }

        private static bool TryParseIlosc(string rawValue, out decimal ilosc)
        {
            ilosc = 0m;
            if (string.IsNullOrWhiteSpace(rawValue))
            {
                return false;
            }

            var trimmed = rawValue.Trim();
            if (decimal.TryParse(trimmed, NumberStyles.Number, CultureInfo.InvariantCulture, out ilosc))
            {
                ilosc = Math.Round(ilosc, 4);
                return true;
            }

            var plCulture = new CultureInfo("pl-PL");
            if (decimal.TryParse(trimmed, NumberStyles.Number, plCulture, out ilosc))
            {
                ilosc = Math.Round(ilosc, 4);
                return true;
            }

            return false;
        }

        private static dynamic PobierzTowarPoSymbolu(dynamic subiekt, string symbol)
        {
            if (string.IsNullOrWhiteSpace(symbol))
            {
                return null;
            }

            var normalized = symbol.Trim();
            try
            {
                var towar = subiekt.Towary.GetType().InvokeMember("Wczytaj", BindingFlags.InvokeMethod, null, subiekt.Towary, new object[] { normalized });
                if (towar != null)
                {
                    return towar;
                }
            }
            catch
            {
            }

            try
            {
                var lista = subiekt.Towary.Wyszukaj(normalized);
                if (lista != null)
                {
                    try
                    {
                        var liczba = (int)lista.Liczba;
                        for (var i = 1; i <= liczba; i++)
                        {
                            var element = lista.Element(i);
                            var symbolTowaru = PobierzWartoscString(element, "Symbol");
                            if (!string.IsNullOrWhiteSpace(symbolTowaru) && string.Equals(symbolTowaru.Trim(), normalized, StringComparison.OrdinalIgnoreCase))
                            {
                                var towarId = PobierzWartoscInt(element, "Id");
                                if (towarId > 0)
                                {
                                    try
                                    {
                                        return subiekt.Towary.GetType().InvokeMember("Wczytaj", BindingFlags.InvokeMethod, null, subiekt.Towary, new object[] { towarId });
                                    }
                                    catch
                                    {
                                        return element;
                                    }
                                }

                                return element;
                            }
                        }
                    }
                    catch
                    {
                    }
                }
            }
            catch
            {
            }

            return null;
        }

        private static void UstawIloscPozycji(dynamic pozycja, decimal ilosc, string symbol, string fzNumer)
        {
            try
            {
                pozycja.GetType().InvokeMember("IloscJm", BindingFlags.SetProperty, null, pozycja, new object[] { ilosc });
                return;
            }
            catch
            {
            }

            try
            {
                pozycja.GetType().InvokeMember("Ilosc", BindingFlags.SetProperty, null, pozycja, new object[] { ilosc });
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: Nie udało się ustawić ilości dla pozycji " + symbol + " w FZ " + fzNumer + ": " + ex.Message);
                _logger.AddLog("ERROR", "Etap 2: błąd ustawienia ilości pozycji", new { symbol = symbol, fzNumer = fzNumer, stackTrace = ex.ToString() });
            }
        }

        private static void UstawMagazynDlaRw(dynamic rw, dynamic subiekt, string fzNumer)
        {
            try
            {
                var magId = PobierzWartoscInt(subiekt, "MagazynId");
                if (magId > 0)
                {
                    rw.GetType().InvokeMember("MagazynId", BindingFlags.SetProperty, null, rw, new object[] { magId });
                    Console.WriteLine("Etap 2: Ustawiono MagazynId=" + magId + " dla FZ " + fzNumer);
                    _logger.AddLog("INFO", "Etap 2: ustawiono MagazynId", new { magazynId = magId, fzNumer = fzNumer });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: Nie udało się ustawić MagazynId dla FZ " + fzNumer + ": " + ex.Message);
                _logger.AddLog("ERROR", "Etap 2: błąd ustawienia MagazynId", new { fzNumer = fzNumer, stackTrace = ex.ToString() });
            }
        }

        private static int PobierzWartoscInt(dynamic obiekt, string nazwa)
        {
            try
            {
                var value = obiekt.GetType().InvokeMember(nazwa, BindingFlags.GetProperty, null, obiekt, null);
                if (value == null)
                {
                    return 0;
                }

                if (value is int intValue)
                {
                    return intValue;
                }

                int parsed;
                if (int.TryParse(value.ToString(), out parsed))
                {
                    return parsed;
                }
            }
            catch
            {
            }

            return 0;
        }

        private static dynamic ZalogujSubiektGT()
        {
            var sferaServer = ConfigurationManager.AppSettings["SferaServer"];
            var sferaDatabase = ConfigurationManager.AppSettings["SferaDatabase"];
            var sferaDbUser = ConfigurationManager.AppSettings["SferaDbUser"];
            var sferaDbPassword = ConfigurationManager.AppSettings["SferaDbPassword"];
            var sferaOperator = ConfigurationManager.AppSettings["SferaOperator"];
            var sferaOperatorPassword = ConfigurationManager.AppSettings["SferaOperatorPassword"];
            var sferaProdukt = ConfigurationManager.AppSettings["SferaProdukt"];
            var sferaAutentykacja = ConfigurationManager.AppSettings["SferaAutentykacja"];
            var sferaUruchomDopasuj = ConfigurationManager.AppSettings["SferaUruchomDopasuj"];
            var sferaUruchomTryb = ConfigurationManager.AppSettings["SferaUruchomTryb"];
            var sferaMagazynId = ConfigurationManager.AppSettings["SferaMagazynId"];

            if (string.IsNullOrWhiteSpace(sferaServer) ||
                string.IsNullOrWhiteSpace(sferaDatabase) ||
                string.IsNullOrWhiteSpace(sferaOperator))
            {
                Console.WriteLine("Brak konfiguracji Sfery w App.config");
                return null;
            }

            Console.WriteLine("Logowanie do Sfery...");

            var gtType = Type.GetTypeFromProgID("InsERT.GT");
            if (gtType == null)
            {
                Console.WriteLine("Brak zarejestrowanego COM: InsERT.GT");
                return null;
            }

            dynamic gt = Activator.CreateInstance(gtType);
            gt.Produkt = ParseIntSetting(sferaProdukt, 1);
            gt.Autentykacja = ParseIntSetting(sferaAutentykacja, 2);
            gt.Serwer = sferaServer;
            gt.Baza = sferaDatabase;

            if (!string.IsNullOrWhiteSpace(sferaDbUser))
            {
                gt.Uzytkownik = sferaDbUser;
            }

            if (!string.IsNullOrWhiteSpace(sferaDbPassword))
            {
                gt.UzytkownikHaslo = sferaDbPassword;
            }

            gt.Operator = sferaOperator;

            if (!string.IsNullOrWhiteSpace(sferaOperatorPassword))
            {
                var dodatkiType = Type.GetTypeFromProgID("InsERT.Dodatki");
                if (dodatkiType != null)
                {
                    dynamic dodatki = Activator.CreateInstance(dodatkiType);
                    gt.OperatorHaslo = dodatki.Szyfruj(sferaOperatorPassword);
                }
                else
                {
                    gt.OperatorHaslo = sferaOperatorPassword;
                }
            }

            dynamic subiekt = gt.Uruchom(ParseIntSetting(sferaUruchomDopasuj, 0), ParseIntSetting(sferaUruchomTryb, 2));
            UkryjOknoSubiekta(subiekt);

            var magazynId = ParseIntSetting(sferaMagazynId, 0);
            if (magazynId > 0)
            {
                subiekt.MagazynId = magazynId;
            }

            return subiekt;
        }

        private static void ZakonczSubiekt(dynamic subiekt)
        {
            try
            {
                subiekt.Zakoncz();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd podczas zamykania Subiekta: " + ex.Message);
            }
        }

        private static int ParseIntSetting(string value, int defaultValue)
        {
            if (int.TryParse(value, out var result))
            {
                return result;
            }

            return defaultValue;
        }

        private static void StartHeartbeat()
        {
            var heartbeatUrl = ConfigurationManager.AppSettings["HeartbeatUrl"];
            if (string.IsNullOrWhiteSpace(heartbeatUrl))
            {
                return;
            }

            _heartbeatTimer = new Timer(_ =>
            {
                try
                {
                    var payloadObj = new
                    {
                        timestamp = DateTime.UtcNow.ToString("o"),
                        status = "ALIVE",
                        source = "ROBOT_SUBIEKT"
                    };

                    var payload = JsonConvert.SerializeObject(payloadObj);
                    using (var httpClient = new HttpClient())
                    using (var content = new StringContent(payload, Encoding.UTF8, "application/json"))
                    {
                        httpClient.PostAsync(heartbeatUrl, content).GetAwaiter().GetResult();
                    }
                }
                catch
                {
                }
            }, null, TimeSpan.Zero, TimeSpan.FromMinutes(5));
        }

        private static UpdateResult PrzetworzDokumenty(string jsonResponse, dynamic subiekt)
        {
            if (subiekt == null)
            {
                Console.WriteLine("Brak połączenia z Subiektem GT");
                return null;
            }

            var dokumenty = JsonConvert.DeserializeObject<List<DokumentZmianyDto>>(jsonResponse) ?? new List<DokumentZmianyDto>();
            var result = new UpdateResult();

            foreach (var dokument in dokumenty)
            {
                try
                {
                    var dok = subiekt.Dokumenty.Wczytaj(dokument.DokId);

                    var liczbaPozycji = dok.Pozycje.Liczba;
                    foreach (var pozycja in dokument.Pozycje)
                    {
                        if (!pozycja.NowaCena.HasValue)
                        {
                            continue;
                        }

                        for (var i = 1; i <= liczbaPozycji; i++)
                        {
                            var poz = dok.Pozycje.Element(i);
                            var symbolTowaru = PobierzSymbolPozycji(poz);
                            if (!string.IsNullOrWhiteSpace(symbolTowaru) && symbolTowaru == pozycja.Symbol)
                            {
                                poz.CenaNettoPrzedRabatem = pozycja.NowaCena.Value;
                            }
                        }
                    }

                    dok.Zapisz();
                    dok.Zamknij();

                    Console.WriteLine("Aktualizacja ZD " + dokument.DokNrPelny + " [OK]");
                    result.Zaktualizowano++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Aktualizacja ZD " + dokument.DokNrPelny + " [BŁĄD] " + ex.Message);
                    result.Bledy++;
                    _logger?.AddLog("ERROR", ex.Message, new { stackTrace = ex.ToString() });
                }
            }

            return result;
        }

        private static string PobierzSymbolPozycji(dynamic pozycja)
        {
            var symbol = PobierzWartoscString(pozycja, "TowarSymbol");
            if (!string.IsNullOrWhiteSpace(symbol))
            {
                return symbol;
            }

            var towar = PobierzWartoscObject(pozycja, "Towar");
            if (towar != null)
            {
                symbol = PobierzWartoscString(towar, "Symbol");
                if (!string.IsNullOrWhiteSpace(symbol))
                {
                    return symbol;
                }
            }

            symbol = PobierzWartoscString(pozycja, "Symbol");
            if (!string.IsNullOrWhiteSpace(symbol))
            {
                return symbol;
            }

            symbol = PobierzWartoscString(pozycja, "SymbolTowaru");
            return symbol;
        }

        private static string PobierzWartoscString(dynamic obiekt, string nazwa)
        {
            try
            {
                var value = obiekt.GetType().InvokeMember(nazwa, BindingFlags.GetProperty, null, obiekt, null);
                return value != null ? value.ToString() : null;
            }
            catch
            {
                return null;
            }
        }

        private static object PobierzWartoscObject(dynamic obiekt, string nazwa)
        {
            try
            {
                return obiekt.GetType().InvokeMember(nazwa, BindingFlags.GetProperty, null, obiekt, null);
            }
            catch
            {
                return null;
            }
        }

        private static bool CzyPoprawnaOdpowiedzWebhooka(string jsonResponse)
        {
            try
            {
                var dokumenty = JsonConvert.DeserializeObject<List<DokumentZmianyDto>>(jsonResponse);
                if (dokumenty == null || dokumenty.Count == 0)
                {
                    return false;
                }

                foreach (var dokument in dokumenty)
                {
                    if (dokument != null && dokument.DokId > 0 && dokument.Pozycje != null && dokument.Pozycje.Count > 0)
                    {
                        return true;
                    }
                }

                return false;
            }
            catch
            {
                return false;
            }
        }

        private class UpdateResult
        {
            public int Zaktualizowano { get; set; }
            public int Bledy { get; set; }
        }

        private static void UkryjOknoSubiekta(dynamic subiekt)
        {
            try
            {
                subiekt.Okno.Widoczne = false;
            }
            catch { }

            try
            {
                var okno = PobierzWartoscObject(subiekt, "Okno");
                if (okno != null)
                {
                    try
                    {
                        okno.GetType().InvokeMember("Ukryj", BindingFlags.InvokeMethod, null, okno, null);
                    }
                    catch { }

                    try
                    {
                        okno.GetType().InvokeMember("Minimalizuj", BindingFlags.InvokeMethod, null, okno, null);
                    }
                    catch { }
                }
            }
            catch { }
        }
    }
}
