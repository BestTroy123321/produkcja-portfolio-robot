using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Runtime.InteropServices;
using System.Threading;
using System.Reflection;
using System.Globalization;
using System.ServiceProcess;
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

    public class RwWebhookResponse
    {
        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("context")]
        public RwResponseContext Context { get; set; }

        [JsonProperty("dane_do_rw")]
        public RwDaneDoRw DaneDoRw { get; set; }
    }

    public class RwResponseContext
    {
        [JsonProperty("fz_id")]
        public int? FzId { get; set; }

        [JsonProperty("fz_numer")]
        public string FzNumer { get; set; }
    }

    public class RwDaneDoRw
    {
        [JsonProperty("opis")]
        public string Opis { get; set; }

        [JsonProperty("pozycje")]
        public List<RwPozycja> Pozycje { get; set; }
    }

    public class RwPozycja
    {
        [JsonProperty("symbol_surowca")]
        public string SymbolSurowca { get; set; }

        [JsonProperty("ilosc_laczna")]
        public object IloscLaczna { get; set; }

        [JsonProperty("cena_jedn")]
        public object CenaJedn { get; set; }

        [JsonProperty("jednostka")]
        public string Jednostka { get; set; }
    }

    public class RwCreateResult
    {
        public bool Success { get; set; }
        public bool IsFatal { get; set; }
        public List<string> Errors { get; set; }
        public string ExceptionDetails { get; set; }
    }

    public class FatalSubiektException : Exception
    {
        public FatalSubiektException(string message) : base(message)
        {
        }
    }

    internal class Program
    {
        private static LoggerService _logger;
        private static Timer _heartbeatTimer;
        private static CancellationTokenSource _cancellationTokenSource;
        private static Thread _workerThread;

        [STAThread]
        private static void Main(string[] args)
        {
            var intervalMinutes = ParseIntSetting(ConfigurationManager.AppSettings["IntervalMinutes"], 30);
            if (intervalMinutes < 1)
            {
                intervalMinutes = 30;
            }

            if (Environment.UserInteractive)
            {
                StartConsole(intervalMinutes);
                return;
            }

            ServiceBase.Run(new SubiektConnectorService(intervalMinutes));
        }

        private static void StartConsole(int intervalMinutes)
        {
            _logger = new LoggerService();
            _logger.AddLog("INFO", "Uruchomienie robota");
            StartHeartbeat();
            RunLoop(intervalMinutes, CancellationToken.None);
        }

        private static void StartService(int intervalMinutes)
        {
            _cancellationTokenSource = new CancellationTokenSource();
            _workerThread = new Thread(() =>
            {
                _logger = new LoggerService();
                _logger.AddLog("INFO", "Uruchomienie robota");
                StartHeartbeat();
                RunLoop(intervalMinutes, _cancellationTokenSource.Token);
            })
            {
                IsBackground = true
            };
            _workerThread.SetApartmentState(ApartmentState.STA);
            _workerThread.Start();
        }

        private static void StopService()
        {
            try
            {
                _cancellationTokenSource?.Cancel();
            }
            catch
            {
            }

            try
            {
                _heartbeatTimer?.Dispose();
            }
            catch
            {
            }

            try
            {
                if (_workerThread != null)
                {
                    _workerThread.Join(TimeSpan.FromSeconds(30));
                }
            }
            catch
            {
            }

            try
            {
                _logger?.FlushAsync().GetAwaiter().GetResult();
            }
            catch
            {
            }
        }

        private static void RunLoop(int intervalMinutes, CancellationToken cancellationToken)
        {
            dynamic subiekt = null;

            while (!cancellationToken.IsCancellationRequested)
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
                            if (cancellationToken.WaitHandle.WaitOne(TimeSpan.FromMinutes(intervalMinutes)))
                            {
                                break;
                            }
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

                if (cancellationToken.WaitHandle.WaitOne(TimeSpan.FromMinutes(intervalMinutes)))
                {
                    break;
                }
            }

            try { ZakonczSubiekt(subiekt); } catch { }
            try { Marshal.ReleaseComObject(subiekt); } catch { }
        }

        private sealed class SubiektConnectorService : ServiceBase
        {
            private readonly int _intervalMinutes;

            public SubiektConnectorService(int intervalMinutes)
            {
                _intervalMinutes = intervalMinutes;
                ServiceName = "ProdukcjaPortfolioSubiektConnectorService";
                CanStop = true;
            }

            protected override void OnStart(string[] args)
            {
                StartService(_intervalMinutes);
            }

            protected override void OnStop()
            {
                StopService();
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
    AND d.dok_DataWyst >= DATEADD(day, -4, GETDATE())
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
            _logger.AddLog("INFO", "Etap 2: rozpoczęto tworzenie RW na podstawie FZ");
            var fzWebhookUrl = ConfigurationManager.AppSettings["FzWebhookUrl"];
            if (string.IsNullOrWhiteSpace(fzWebhookUrl))
            {
                Console.WriteLine("Etap 2: Brak ustawienia appSettings: FzWebhookUrl");
                _logger.AddLog("ERROR", "Etap 2: Brak ustawienia appSettings: FzWebhookUrl", new { stackTrace = Environment.StackTrace });
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
                _logger.AddLog("ERROR", "Etap 2: Błąd podczas pobierania FZ", new { stackTrace = ex.ToString() });
                return;
            }

            if (dokumenty.Count == 0)
            {
                Console.WriteLine("Etap 2: Brak FZ do przetworzenia, wysyłam pustą listę do webhooka.");
            }
            else
            {
                Console.WriteLine("Etap 2: Znaleziono " + dokumenty.Count + " FZ z " + liczbaPozycji + " produktami.");
            }
            _logger.AddLog("SUCCESS", "Etap 2: Pobrano " + liczbaPozycji + " pozycji z " + dokumenty.Count + " dokumentów", new { liczbaPozycji, liczbaDokumentow = dokumenty.Count });

            var payload = JsonConvert.SerializeObject(new List<FzPayload>(dokumenty.Values));
            try
            {
                using (var httpClient = new HttpClient())
                using (var content = new StringContent(payload, Encoding.UTF8, "application/json"))
                {
                    Console.WriteLine("Etap 2: Webhook URL: " + fzWebhookUrl);
                    Console.WriteLine("Etap 2: Wysyłanie do webhooka, rozmiar payloadu: " + payload.Length);
                    var response = httpClient.PostAsync(fzWebhookUrl, content).GetAwaiter().GetResult();
                    var responseBody = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                    Console.WriteLine("Etap 2: Odpowiedź webhooka: " + (int)response.StatusCode + " " + response.ReasonPhrase);
                    Console.WriteLine("Etap 2: Rozmiar odpowiedzi: " + (responseBody == null ? 0 : responseBody.Length));
                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        Console.WriteLine("Etap 2: Webhook zwrócił błędny status, pomijam tworzenie RW.");
                        _logger.AddLog("ERROR", "Etap 2: Webhook zwrócił błędny status", new { status = (int)response.StatusCode, responseBody });
                        return;
                    }
                    if (string.IsNullOrWhiteSpace(responseBody))
                    {
                        Console.WriteLine("Etap 2: Webhook nie zwrócił treści, pomijam tworzenie RW.");
                        _logger.AddLog("INFO", "Etap 2: Webhook nie zwrócił treści");
                        return;
                    }

                    List<RwWebhookResponse> odpowiedzRw;
                    try
                    {
                        odpowiedzRw = JsonConvert.DeserializeObject<List<RwWebhookResponse>>(responseBody);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Etap 2: Błąd parsowania odpowiedzi webhooka: " + ex.Message);
                        _logger.AddLog("ERROR", "Etap 2: Błąd parsowania odpowiedzi webhooka", new { stackTrace = ex.ToString(), responseBody });
                        return;
                    }

                    if (odpowiedzRw == null || odpowiedzRw.Count == 0)
                    {
                        Console.WriteLine("Etap 2: Webhook nie zwrócił pozycji RW.");
                        _logger.AddLog("ERROR", "Etap 2: Webhook nie zwrócił pozycji RW", new { responseBody });
                        return;
                    }

                    var liczbaRw = 0;
                    var liczbaPozycjiRw = 0;
                    foreach (var item in odpowiedzRw)
                    {
                        if (item == null)
                        {
                            continue;
                        }
                        if (!string.Equals(item.Status, "success", StringComparison.OrdinalIgnoreCase))
                        {
                            Console.WriteLine("Etap 2: RW: status != success, pomijam. FZ: " + item?.Context?.FzNumer);
                            _logger.AddLog("ERROR", "Etap 2: RW: status != success", new { fzNumer = item?.Context?.FzNumer, status = item?.Status });
                            continue;
                        }
                        if (item.DaneDoRw == null || item.DaneDoRw.Pozycje == null || item.DaneDoRw.Pozycje.Count == 0)
                        {
                            Console.WriteLine("Etap 2: RW: brak danych do utworzenia RW. FZ: " + item?.Context?.FzNumer);
                            _logger.AddLog("ERROR", "Etap 2: RW: brak danych do utworzenia RW", new { fzNumer = item?.Context?.FzNumer });
                            continue;
                        }

                        Console.WriteLine("Etap 2: RW: Tworzenie RW dla FZ: " + item?.Context?.FzNumer);
                        var result = UtworzDokumentRW(subiekt, connectionString, item.DaneDoRw.Opis, item.DaneDoRw.Pozycje);
                        if (result == null)
                        {
                            _logger.AddLog("ERROR", "Etap 2: RW: błąd dla FZ", new { fzNumer = item?.Context?.FzNumer, errors = new[] { "Nieznany błąd tworzenia RW" } });
                            continue;
                        }
                        if (result.IsFatal)
                        {
                            _logger.AddLog("ERROR", "Etap 2: błąd krytyczny podczas tworzenia RW", new { fzNumer = item?.Context?.FzNumer, errors = result.Errors, exception = result.ExceptionDetails });
                            throw new FatalSubiektException("Etap 2: błąd krytyczny podczas tworzenia RW");
                        }
                        if (!result.Success)
                        {
                            _logger.AddLog("ERROR", "Etap 2: RW: błąd dla FZ", new { fzNumer = item?.Context?.FzNumer, errors = result.Errors, exception = result.ExceptionDetails });
                            Console.WriteLine("Etap 2: RW: błąd dla FZ: " + item?.Context?.FzNumer);
                            continue;
                        }

                        liczbaRw++;
                        liczbaPozycjiRw += item.DaneDoRw.Pozycje.Count;
                    }

                    _logger.AddLog("SUCCESS", "Etap 2: Wystawiono " + liczbaRw + " RW dla " + liczbaPozycjiRw + " pozycji", new { liczbaRw, liczbaPozycji = liczbaPozycjiRw });
                }
            }
            catch (FatalSubiektException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: Błąd podczas wysyłki webhooka: " + ex.Message);
                _logger.AddLog("ERROR", "Etap 2: Błąd podczas wysyłki webhooka", new { stackTrace = ex.ToString() });
            }
        }

        private static RwCreateResult UtworzDokumentRW(dynamic subiekt, string connectionString, string opis, List<RwPozycja> pozycje)
        {
            var result = new RwCreateResult
            {
                Success = false,
                IsFatal = false,
                Errors = new List<string>(),
                ExceptionDetails = null
            };
            try
            {
                if (subiekt == null)
                {
                    Console.WriteLine("Etap 2: RW: Brak połączenia z Subiektem GT");
                    result.Errors.Add("Brak połączenia z Subiektem GT");
                    result.IsFatal = true;
                    return result;
                }

                Console.WriteLine("Etap 2: RW: Start");
                if (pozycje == null || pozycje.Count == 0)
                {
                    Console.WriteLine("Etap 2: RW: Brak pozycji do dodania");
                    result.Errors.Add("Brak pozycji do dodania");
                    return result;
                }

                Console.WriteLine("Etap 2: RW: Tworzenie dokumentu RW");
                dynamic dok = UtworzDokumentRwRoboczy(subiekt);
                if (dok == null)
                {
                    Console.WriteLine("Etap 2: RW: Nie udało się utworzyć dokumentu RW");
                    result.Errors.Add("Nie udało się utworzyć dokumentu RW");
                    result.IsFatal = true;
                    return result;
                }
                Console.WriteLine("Etap 2: RW: Dokument RW utworzony w buforze");
                try
                {
                    dok.Uwagi = opis ?? string.Empty;
                    Console.WriteLine("Etap 2: RW: Ustawiono uwagi");
                }
                catch
                {
                    Console.WriteLine("Etap 2: RW: Nie udało się ustawić uwag");
                }

                foreach (var pozycja in pozycje)
                {
                    var symbol = pozycja?.SymbolSurowca;
                    if (string.IsNullOrWhiteSpace(symbol))
                    {
                        Console.WriteLine("Etap 2: RW: Pominięto pozycję bez symbolu");
                        result.Errors.Add("Pominięto pozycję bez symbolu");
                        continue;
                    }

                    if (!TryParseDecimal(pozycja?.IloscLaczna, out var ilosc))
                    {
                        Console.WriteLine("Etap 2: RW: Nieprawidłowa ilość dla: " + symbol);
                        result.Errors.Add("Nieprawidłowa ilość dla: " + symbol);
                        continue;
                    }

                    Console.WriteLine("Etap 2: RW: Dodawanie pozycji: " + symbol + " | " + ilosc);
                    var towar = PobierzTowarPoSymbolu(subiekt, symbol);
                    if (towar == null)
                    {
                        Console.WriteLine("Etap 2: RW: Towar nie znaleziony: " + symbol);
                        result.Errors.Add("Towar nie znaleziony: " + symbol);
                        continue;
                    }

                    var nazwaTowaru = PobierzWartoscString(towar, "Nazwa");
                    if (string.IsNullOrWhiteSpace(nazwaTowaru))
                    {
                        nazwaTowaru = PobierzWartoscString(towar, "NazwaPelna");
                    }
                    Console.WriteLine("Etap 2: RW: Towar znaleziony: " + symbol + (string.IsNullOrWhiteSpace(nazwaTowaru) ? "" : " | " + nazwaTowaru));

                    int? towarId = PobierzTowarId((object)towar, connectionString, symbol);
                    if (!towarId.HasValue)
                    {
                        Console.WriteLine("Etap 2: RW: Nie udało się ustalić tw_Id dla: " + symbol);
                        result.Errors.Add("Nie udało się ustalić tw_Id dla: " + symbol);
                        continue;
                    }
                    Console.WriteLine("Etap 2: RW: Ustalono tw_Id: " + towarId.Value);

                    dynamic poz = DodajPozycjeRw(dok, towarId.Value);
                    if (poz == null)
                    {
                        Console.WriteLine("Etap 2: RW: Nie udało się dodać pozycji: " + symbol);
                        result.Errors.Add("Nie udało się dodać pozycji: " + symbol);
                        continue;
                    }
                    Console.WriteLine("Etap 2: RW: Pozycja dodana: " + symbol);

                    try
                    {
                        poz.IloscJm = ilosc;
                        Console.WriteLine("Etap 2: RW: Ustawiono IloscJm: " + ilosc);
                    }
                    catch
                    {
                        Console.WriteLine("Etap 2: RW: Nie udało się ustawić IloscJm dla: " + symbol);
                        result.Errors.Add("Nie udało się ustawić IloscJm dla: " + symbol);
                    }

                    var cenaJednValue = pozycja?.CenaJedn;
                    if (cenaJednValue == null)
                    {
                        Console.WriteLine("Etap 2: RW: Brak cena_jedn dla: " + symbol);
                        result.Errors.Add("Brak cena_jedn dla: " + symbol);
                        continue;
                    }
                    if (!TryParseDecimal(cenaJednValue, out var cenaJedn))
                    {
                        Console.WriteLine("Etap 2: RW: Nieprawidłowa cena_jedn dla: " + symbol);
                        result.Errors.Add("Nieprawidłowa cena_jedn dla: " + symbol);
                        continue;
                    }

                    string bladCeny;
                    if (!TryUstawCeneJedn(poz, cenaJedn, out bladCeny))
                    {
                        Console.WriteLine("Etap 2: RW: Nie udało się ustawić ceny dla: " + symbol);
                        result.Errors.Add(bladCeny);
                        continue;
                    }
                    Console.WriteLine("Etap 2: RW: Ustawiono cenę: " + cenaJedn);
                }

                if (result.Errors.Count > 0)
                {
                    try { dok.Zamknij(); } catch { }
                    return result;
                }

                Console.WriteLine("Etap 2: RW: Zapis dokumentu");
                try
                {
                    dok.Zapisz();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Etap 2: Błąd zapisu RW: " + ex.Message);
                    result.Errors.Add(ex.Message);
                    result.ExceptionDetails = ex.ToString();
                    result.IsFatal = IsFatalSubiektError(ex);
                    return result;
                }
                Console.WriteLine("Etap 2: RW: Zapis OK");
                try { dok.Zamknij(); } catch { }
                Console.WriteLine("Etap 2: RW: Dokument zamknięty");
                result.Success = true;
                return result;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: Błąd tworzenia RW: " + ex.ToString());
                result.Errors.Add(ex.Message);
                result.ExceptionDetails = ex.ToString();
                result.IsFatal = IsFatalSubiektError(ex);
                return result;
            }
        }

        private static bool IsFatalSubiektError(Exception ex)
        {
            if (ex == null)
            {
                return false;
            }

            var message = ex.ToString();
            if (message.IndexOf("RPC", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }
            if (message.IndexOf("Sfera", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }
            if (message.IndexOf("Subiekt", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }
            if (message.IndexOf("Klasa nie jest zarejestrowana", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }
            if (message.IndexOf("Class not registered", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }

            return false;
        }

        private static dynamic UtworzDokumentRwRoboczy(dynamic subiekt)
        {
            dynamic dokumenty = null;
            try
            {
                dokumenty = PobierzWartoscObject(subiekt, "Dokumenty");
            }
            catch
            {
                dokumenty = null;
            }

            if (dokumenty == null)
            {
                try
                {
                    dokumenty = subiekt.Dokumenty;
                }
                catch
                {
                    Console.WriteLine("Etap 2: RW: Nie można pobrać kolekcji Dokumenty");
                    return null;
                }
            }

            dynamic dok = null;

            try
            {
                Console.WriteLine("Etap 2: RW: Próba utworzenia RW: Dodaj(-6)");
                dok = dokumenty.Dodaj(-6);
                Console.WriteLine("Etap 2: RW: Dodaj(-6) OK");
                return dok;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: RW: Dodaj(-6) niepowodzenie: " + ex.Message);
                return null;
            }
        }

        private static dynamic PobierzTowarPoSymbolu(dynamic subiekt, string symbol)
        {
            try
            {
                var towary = PobierzWartoscObject(subiekt, "Towary");
                if (towary == null)
                {
                    Console.WriteLine("Etap 2: RW: Nie można pobrać kolekcji Towary");
                    return null;
                }

                try
                {
                    var towar = towary.GetType().InvokeMember("Wczytaj", BindingFlags.InvokeMethod, null, towary, new object[] { symbol });
                    if (towar != null)
                    {
                        Console.WriteLine("Etap 2: RW: Wczytaj(symbol) zwrócił towar");
                        return towar;
                    }
                }
                catch
                {
                    Console.WriteLine("Etap 2: RW: Wczytaj(symbol) nie powiodło się");
                }

                try
                {
                    var towar = towary.GetType().InvokeMember("WczytajPoSymbolu", BindingFlags.InvokeMethod, null, towary, new object[] { symbol });
                    if (towar != null)
                    {
                        Console.WriteLine("Etap 2: RW: WczytajPoSymbolu(symbol) zwrócił towar");
                        return towar;
                    }
                }
                catch
                {
                    Console.WriteLine("Etap 2: RW: WczytajPoSymbolu(symbol) nie powiodło się");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: RW: Błąd pobierania towaru: " + ex.Message);
            }

            return null;
        }

        private static int? PobierzTowarId(object towar, string connectionString, string symbol)
        {
            object towarObj = towar;
            if (towarObj == null)
            {
                return null;
            }

            var towarId = PobierzWartoscInt(towarObj, "Id");
            if (towarId.HasValue)
            {
                Console.WriteLine("Etap 2: RW: tw_Id z obiektu Towar.Id: " + towarId.Value);
                return towarId;
            }

            towarId = PobierzWartoscInt(towarObj, "Identyfikator");
            if (towarId.HasValue)
            {
                Console.WriteLine("Etap 2: RW: tw_Id z obiektu Towar.Identyfikator: " + towarId.Value);
                return towarId;
            }

            towarId = PobierzWartoscInt(towarObj, "TowarId");
            if (towarId.HasValue)
            {
                Console.WriteLine("Etap 2: RW: tw_Id z obiektu Towar.TowarId: " + towarId.Value);
                return towarId;
            }

            towarId = PobierzWartoscInt(towarObj, "IdTowaru");
            if (towarId.HasValue)
            {
                Console.WriteLine("Etap 2: RW: tw_Id z obiektu Towar.IdTowaru: " + towarId.Value);
                return towarId;
            }

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                Console.WriteLine("Etap 2: RW: Brak connection stringa do pobrania tw_Id");
                return null;
            }

            try
            {
                using (var connection = new SqlConnection(connectionString))
                using (var command = new SqlCommand("SELECT TOP 1 tw_Id FROM tw__Towar WHERE tw_Symbol = @symbol", connection))
                {
                    command.Parameters.AddWithValue("@symbol", symbol);
                    connection.Open();
                    var result = command.ExecuteScalar();
                    if (result != null && int.TryParse(result.ToString(), out var id))
                    {
                        Console.WriteLine("Etap 2: RW: tw_Id z bazy: " + id);
                        return id;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: RW: Błąd pobierania tw_Id z bazy: " + ex.Message);
            }

            return null;
        }

        private static dynamic DodajPozycjeRw(dynamic dokument, int towarId)
        {
            dynamic pozycje = null;
            try
            {
                pozycje = PobierzWartoscObject(dokument, "Pozycje");
            }
            catch
            {
                pozycje = null;
            }

            if (pozycje == null)
            {
                try
                {
                    pozycje = dokument.Pozycje;
                }
                catch
                {
                    Console.WriteLine("Etap 2: RW: Nie można pobrać kolekcji Pozycje");
                    return null;
                }
            }

            try
            {
                Console.WriteLine("Etap 2: RW: Próba dodania pozycji: Dodaj(tw_Id)");
                var poz = pozycje.GetType().InvokeMember("Dodaj", BindingFlags.InvokeMethod, null, pozycje, new object[] { towarId });
                Console.WriteLine("Etap 2: RW: Dodaj(tw_Id) OK");
                return poz;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Etap 2: RW: Dodaj(tw_Id) niepowodzenie: " + ex.Message);
            }

            return null;
        }

        private static bool TryUstawCeneJedn(dynamic pozycja, decimal cena, out string error)
        {
            error = null;
            if (pozycja == null)
            {
                error = "Brak obiektu pozycji do ustawienia ceny";
                return false;
            }

            var pola = new[]
            {
                "CenaNettoPrzedRabatem",
                "CenaNetto",
                "Cena",
                "CenaJedn",
                "CenaJednostkowa",
                "CenaNettoJednostkowa"
            };

            foreach (var pole in pola)
            {
                if (UstawWartoscDecimal(pozycja, pole, cena))
                {
                    return true;
                }
            }

            error = "Nie udało się ustawić ceny (cena_jedn)";
            return false;
        }

        private static bool UstawWartoscDecimal(dynamic obiekt, string nazwa, decimal wartosc)
        {
            try
            {
                object obj = obiekt;
                if (obj == null)
                {
                    return false;
                }
                obj.GetType().InvokeMember(nazwa, BindingFlags.SetProperty, null, obj, new object[] { wartosc });
                return true;
            }
            catch
            {
                return false;
            }
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

            dynamic subiekt = null;
            try
            {
                subiekt = gt.Uruchom(ParseIntSetting(sferaUruchomDopasuj, 0), ParseIntSetting(sferaUruchomTryb, 2));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Nie udalo sie uruchomic Subiekta: " + ex.Message);
                _logger?.AddLog("ERROR", "Nie udalo sie uruchomic Subiekta", new { stackTrace = ex.ToString() });
                return null;
            }

            if (subiekt == null)
            {
                Console.WriteLine("Uruchomienie Subiekta zwrocilo null");
                _logger?.AddLog("ERROR", "Uruchomienie Subiekta zwrocilo null");
                return null;
            }

            UkryjOknoSubiekta(subiekt);

            var magazynId = ParseIntSetting(sferaMagazynId, 0);
            if (magazynId > 0)
            {
                try
                {
                    subiekt.MagazynId = magazynId;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Nie mozna ustawic MagazynId: " + ex.Message);
                    _logger?.AddLog("ERROR", "Nie mozna ustawic MagazynId", new { stackTrace = ex.ToString() });
                }
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
                object obj = obiekt;
                if (obj == null)
                {
                    return null;
                }
                var value = obj.GetType().InvokeMember(nazwa, BindingFlags.GetProperty, null, obj, null);
                return value != null ? value.ToString() : null;
            }
            catch
            {
                return null;
            }
        }

        private static int? PobierzWartoscInt(object obiekt, string nazwa)
        {
            try
            {
                object obj = obiekt;
                if (obj == null)
                {
                    return null;
                }
                var value = obj.GetType().InvokeMember(nazwa, BindingFlags.GetProperty, null, obj, null);
                if (value == null)
                {
                    return null;
                }
                int result;
                if (int.TryParse(value.ToString(), out result))
                {
                    return result;
                }
                return null;
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
                object obj = obiekt;
                if (obj == null)
                {
                    return null;
                }
                return obj.GetType().InvokeMember(nazwa, BindingFlags.GetProperty, null, obj, null);
            }
            catch
            {
                return null;
            }
        }

        private static bool TryParseDecimal(object value, out decimal result)
        {
            result = 0m;
            if (value == null)
            {
                return false;
            }

            if (value is decimal decimalValue)
            {
                result = decimalValue;
                return true;
            }

            if (value is double doubleValue)
            {
                result = Convert.ToDecimal(doubleValue);
                return true;
            }

            if (value is float floatValue)
            {
                result = Convert.ToDecimal(floatValue);
                return true;
            }

            var text = value.ToString();
            if (string.IsNullOrWhiteSpace(text))
            {
                return false;
            }

            if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out result))
            {
                return true;
            }

            var normalized = text.Replace(",", ".");
            if (decimal.TryParse(normalized, NumberStyles.Any, CultureInfo.InvariantCulture, out result))
            {
                return true;
            }

            if (decimal.TryParse(text, NumberStyles.Any, new CultureInfo("pl-PL"), out result))
            {
                return true;
            }

            return false;
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
