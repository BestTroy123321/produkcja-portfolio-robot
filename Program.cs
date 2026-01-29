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
            var n8nUrl = ConfigurationManager.AppSettings["N8nUrl"];

            Console.WriteLine("Łączenie z bazą...");

            _logger.AddLog("INFO", "Rozpoczęto synchronizację");

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                Console.WriteLine("Brak connection stringa: SubiektDB");
                _logger.AddLog("ERROR", "Brak connection stringa: SubiektDB", new { stackTrace = Environment.StackTrace });
                return;
            }

            if (string.IsNullOrWhiteSpace(n8nUrl))
            {
                Console.WriteLine("Brak ustawienia appSettings: N8nUrl");
                _logger.AddLog("ERROR", "Brak ustawienia appSettings: N8nUrl", new { stackTrace = Environment.StackTrace });
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
                var response = httpClient.PostAsync(n8nUrl, content).GetAwaiter().GetResult();
                var responseBody = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

                Console.WriteLine("Odpowiedź serwera: " + (int)response.StatusCode + " " + response.ReasonPhrase);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    Console.WriteLine("Webhook zwrócił błędny status, kończę działanie.");
                    _logger.AddLog("ERROR", "Webhook zwrócił błędny status", new { stackTrace = responseBody });
                    return;
                }

                if (string.IsNullOrWhiteSpace(responseBody))
                {
                    Console.WriteLine("Webhook nie zwrócił treści, kończę działanie.");
                    _logger.AddLog("INFO", "Webhook nie zwrócił treści");
                    return;
                }

                Console.WriteLine("Treść odpowiedzi: " + responseBody);
                if (!CzyPoprawnaOdpowiedzWebhooka(responseBody))
                {
                    Console.WriteLine("Webhook nie zwrócił wymaganych danych, kończę działanie.");
                    _logger.AddLog("ERROR", "Webhook nie zwrócił wymaganych danych", new { stackTrace = responseBody });
                    return;
                }

                var result = PrzetworzDokumenty(responseBody, subiekt);
                if (result != null)
                {
                    _logger.AddLog("SUCCESS", "Podsumowanie aktualizacji", new { zaktualizowano = result.Zaktualizowano, bledy = result.Bledy });
                }
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
