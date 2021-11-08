using Newtonsoft.Json.Linq;
using System.Collections.Generic;

#pragma warning disable 1591

namespace Frends.Community.Apache
{
    public class Config
    {
        private readonly Dictionary<string, string> _config;

        /// <summary>
        /// Creates config data structure for later use
        /// </summary>
        /// <param name="json">JSON configuration</param>
        /// <returns>Config dictionary</returns>
        public Config(JToken json)
        {
            _config = new Dictionary<string, string>();

            // A simple key-value store
            foreach (var element in json)
            {
                string name = element.Value<string>("name");
                string format = element.Value<string>("format");
                string culture = element.Value<string>("culture");

                if (string.IsNullOrWhiteSpace(format))
                {
                    format = "";
                }

                // Culture overwrites format. (decimals, floats and doubles)
                if (!string.IsNullOrWhiteSpace(culture))
                {
                    format = culture;
                }

                _config[name] = format;
            }
        }

        /// <summary>
        /// Gets value from config using key - for future use
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns>Value</returns>
        public string GetConfigValue(string key)
        {
            return _config[key];
        }
    }
}
