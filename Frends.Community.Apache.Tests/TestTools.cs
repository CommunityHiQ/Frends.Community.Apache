using System;
using System.IO;
using System.Security.Cryptography;
using System.Globalization;

namespace Frends.Community.Apache.Tests
{
    class TestTools
    {
        /// <summary>
        /// Read file and compute MD5 hash
        /// Using MD5 because it is short and it is enough unique
        /// </summary>
        /// <param name="filename">Full path to file</param>
        /// <returns>MD5 hash</returns>
        public static string MD5Hash(string filename)
        {
            using var md5 = MD5.Create();
            using FileStream stream = File.OpenRead(filename);

            var hash = md5.ComputeHash(stream);

            return BitConverter.ToString(hash).Replace("-", string.Empty).ToLower(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Removes  file if it exists
        /// </summary>
        /// <param name="filepath">Full filepath</param>
        /// 
        public static void RemoveOutputFile(string filepath)
        {
            if (File.Exists(filepath))
            {
                File.Delete(filepath);
            }
        }
    }
}
