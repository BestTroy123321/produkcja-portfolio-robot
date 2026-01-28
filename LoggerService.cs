using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;

namespace SubiektConnector
{
    public class LoggerService
    {
        private readonly List<LogEntry> _logs;
        private readonly string _logWebhookUrl;
        private readonly string _pendingFilePath;

        public LoggerService()
        {
            _logWebhookUrl = ConfigurationManager.AppSettings["LogWebhookUrl"];
            _pendingFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "pending_logs.json");
            _logs = new List<LogEntry>();

            if (File.Exists(_pendingFilePath))
            {
                try
                {
                    var content = File.ReadAllText(_pendingFilePath);
                    var pending = JsonConvert.DeserializeObject<List<LogEntry>>(content);
                    if (pending != null && pending.Count > 0)
                    {
                        _logs.AddRange(pending);
                    }
                }
                catch
                {
                }
            }
        }

        public void AddLog(string level, string message, object details = null)
        {
            var entry = new LogEntry
            {
                Timestamp = DateTime.UtcNow.ToString("o"),
                Level = level,
                Source = "ROBOT_SUBIEKT",
                Message = message,
                Details = details
            };
            _logs.Add(entry);
        }

        public async System.Threading.Tasks.Task FlushAsync()
        {
            if (_logs.Count == 0)
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(_logWebhookUrl))
            {
                ZapiszPending();
                return;
            }

            var payload = JsonConvert.SerializeObject(_logs);
            try
            {
                using (var httpClient = new HttpClient())
                using (var content = new StringContent(payload, Encoding.UTF8, "application/json"))
                {
                    var response = await httpClient.PostAsync(_logWebhookUrl, content);
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        if (File.Exists(_pendingFilePath))
                        {
                            File.Delete(_pendingFilePath);
                        }
                        _logs.Clear();
                        return;
                    }
                }
            }
            catch
            {
            }

            ZapiszPending();
        }

        private void ZapiszPending()
        {
            try
            {
                var content = JsonConvert.SerializeObject(_logs);
                File.WriteAllText(_pendingFilePath, content);
            }
            catch
            {
            }
        }

        private class LogEntry
        {
            [JsonProperty("timestamp")]
            public string Timestamp { get; set; }

            [JsonProperty("level")]
            public string Level { get; set; }

            [JsonProperty("source")]
            public string Source { get; set; }

            [JsonProperty("message")]
            public string Message { get; set; }

            [JsonProperty("details")]
            public object Details { get; set; }
        }
    }
}
